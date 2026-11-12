package workload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type recoveryWorkload struct{ launch LaunchSpec }

func (*recoveryWorkload) Type() string                         { return "test" }
func (*recoveryWorkload) Supports(string) bool                 { return true }
func (*recoveryWorkload) Validate(context.Context, Spec) error { return nil }
func (w *recoveryWorkload) Prepare(context.Context, Spec, Placement) (LaunchSpec, error) {
	return w.launch, nil
}
func recoveryDefinition() *recoveryWorkload {
	return &recoveryWorkload{LaunchSpec{Image: "server:v1", Executable: "server", Arguments: []string{"serve"}, Environment: []string{"MODE=one"}}}
}

// A stateful command fake exercises creation, recovery, replacement and deletion
// through the same paths. It never calls a Docker daemon or Kubernetes cluster.
type runtimeFake struct {
	mu                                                         sync.Mutex
	calls                                                      []string
	container                                                  *dockerInspection
	pod                                                        map[string]any
	containersCreated, podsCreated                             int
	failAPI, failStop, loseCreate, holdDelete, replaceOnDelete bool
}

func (f *runtimeFake) Output(ctx context.Context, input []byte, binary string, args ...string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	call := strings.Join(args, " ")
	f.calls = append(f.calls, call)
	if f.failAPI {
		return "", errors.New("runtime unavailable")
	}
	if binary == "docker" {
		switch {
		case len(args) > 0 && args[0] == "ps":
			if f.container == nil {
				return "", nil
			}
			filter := valueAfter(args, "--filter")
			if strings.HasPrefix(filter, "id=") && filter != "id="+f.container.ID {
				return "", nil
			}
			if strings.HasPrefix(filter, "name=") && filter != "name=^"+f.container.Name+"$" {
				return "", nil
			}
			return f.container.ID, nil
		case len(args) > 0 && args[0] == "inspect":
			if f.container == nil {
				return "", errors.New("absent")
			}
			return encoded(f.container), nil
		case len(args) > 0 && args[0] == "run":
			f.containersCreated++
			v := &dockerInspection{}
			v.ID = fmt.Sprint("container-", f.containersCreated)
			v.Name = "/" + valueAfter(args, "--name")
			v.Config.Labels = map[string]string{}
			for i, a := range args {
				if a == "--label" {
					k, vv, _ := strings.Cut(args[i+1], "=")
					v.Config.Labels[k] = vv
				}
			}
			for i, a := range args {
				if a == "--entrypoint" {
					v.Config.Image = args[i+2]
					v.Config.Entrypoint = []string{args[i+1]}
					v.Config.Cmd = append([]string(nil), args[i+3:]...)
				}
				if a == "--env" {
					v.Config.Env = append(v.Config.Env, args[i+1])
				}
				if a == "--workdir" {
					v.Config.WorkingDir = args[i+1]
				}
				if a == "--memory" {
					v.HostConfig.Memory, _ = strconv.ParseInt(args[i+1], 10, 64)
				}
				if a == "--cpus" {
					cpu, _ := strconv.ParseFloat(args[i+1], 64)
					v.HostConfig.NanoCpus = int64(cpu * 1e9)
				}
				if a == "--pids-limit" {
					limit, _ := strconv.ParseInt(args[i+1], 10, 64)
					v.HostConfig.PidsLimit = &limit
				}
				if a == "--network" {
					v.HostConfig.NetworkMode = args[i+1]
				}
			}
			v.State.Running = true
			v.State.Pid = 42
			v.NetworkSettings.Ports = map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			}{}
			for i, a := range args {
				if a == "--publish" {
					parts := strings.Split(args[i+1], ":")
					port := parts[len(parts)-1]
					v.NetworkSettings.Ports[port+"/tcp"] = []struct {
						HostIP   string `json:"HostIp"`
						HostPort string `json:"HostPort"`
					}{{"127.0.0.1", port}}
				}
			}
			f.container = v
			if f.loseCreate {
				return "", errors.New("creation response lost")
			}
			return v.ID, nil
		case len(args) > 0 && args[0] == "stop":
			if f.failStop {
				return "", errors.New("daemon stop failed")
			}
			if f.container != nil {
				f.container.State.Running = false
			}
			return "", nil
		case len(args) > 0 && args[0] == "rm":
			if f.failStop {
				return "", errors.New("daemon remove failed")
			}
			f.container = nil
			return "", nil
		}
	}
	switch {
	case strings.Contains(call, "get pods "):
		items := []any{}
		if f.pod != nil {
			items = append(items, f.pod)
		}
		return encoded(map[string]any{"items": items}), nil
	case strings.Contains(call, "get pod "):
		if f.pod == nil {
			return "", nil
		}
		return encoded(f.pod), nil
	case strings.Contains(call, "create --filename -"):
		f.podsCreated++
		if err := json.Unmarshal(input, &f.pod); err != nil {
			return "", err
		}
		f.pod["metadata"].(map[string]any)["uid"] = fmt.Sprint("pod-", f.podsCreated)
		f.pod["status"] = map[string]any{"phase": "Running", "podIP": "127.0.0.1", "containerStatuses": []any{map[string]any{"ready": true}}}
		if f.loseCreate {
			return "", errors.New("creation response lost")
		}
		return encoded(f.pod), nil
	case strings.Contains(call, "delete --raw "):
		if f.failStop {
			return "", errors.New("delete failed")
		}
		if f.replaceOnDelete {
			f.pod["metadata"].(map[string]any)["uid"] = "replacement"
		}
		var body map[string]any
		if err := json.Unmarshal(input, &body); err != nil {
			return "", err
		}
		if f.pod != nil && body["preconditions"].(map[string]any)["uid"] != f.pod["metadata"].(map[string]any)["uid"] {
			return "", errors.New("UID precondition failed")
		}
		if !f.holdDelete {
			f.pod = nil
		}
		return "", nil
	}
	return "", fmt.Errorf("unexpected command: %s %s", binary, call)
}
func encoded(v any) string { b, _ := json.Marshal(v); return string(b) }
func valueAfter(args []string, key string) string {
	for i, a := range args {
		if a == key && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}
func (f *runtimeFake) change(fn func()) { f.mu.Lock(); defer f.mu.Unlock(); fn() }
func (f *runtimeFake) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.containersCreated, f.podsCreated
}
func testDocker(t *testing.T, f *runtimeFake) *DockerDriver {
	t.Helper()
	d, err := newDockerDriver(DockerDriverConfig{Binary: "docker", NodeID: "node-a", PublishHost: "127.0.0.1", StartTimeout: time.Second, StopTimeout: time.Second}, f)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func testKube(t *testing.T, f *runtimeFake) *KubernetesDriver {
	t.Helper()
	d, err := newKubernetesDriver(KubernetesDriverConfig{KubectlBinary: "kubectl", NodeID: "node-a", Namespace: "test", ImagePullPolicy: "IfNotPresent", RouteMode: "pod-ip", StartTimeout: time.Second, StopTimeout: time.Second}, f)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func stopTestProcess(t *testing.T, p Process) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDockerRecoveryChecksCompleteLaunch(t *testing.T) {
	for _, change := range []string{"revision", "arguments", "environment", "limits", "directory", "image", "actual-command", "actual-limits"} {
		t.Run(change, func(t *testing.T) {
			f := &runtimeFake{}
			d := testDocker(t, f)
			s := validSpec()
			s.Driver = DriverDocker
			definition := recoveryDefinition()
			if change == "actual-limits" {
				definition.launch.Limits.MemoryBytes = 1024
			}
			p, err := d.Start(t.Context(), s, definition)
			if err != nil {
				t.Fatal(err)
			}
			adopted, err := testDocker(t, f).Start(t.Context(), s, definition)
			if err != nil {
				t.Fatal(err)
			}
			a, _ := f.counts()
			if a != 1 || adopted.PID() != p.PID() {
				t.Fatal("matching launch was not adopted")
			}
			switch change {
			case "revision":
				s.Revision++
			case "arguments":
				definition.launch.Arguments = []string{"different"}
			case "environment":
				definition.launch.Environment = []string{"MODE=two"}
			case "limits":
				definition.launch.Limits.MemoryBytes = 1024
			case "directory":
				definition.launch.WorkingDirectory = "/new"
			case "image":
				definition.launch.Image = "server:v2"
			case "actual-command":
				f.change(func() { f.container.Config.Cmd = []string{"mutated"} })
			case "actual-limits":
				f.change(func() { f.container.HostConfig.Memory = 512 })
			}
			replacement, err := testDocker(t, f).Start(t.Context(), s, definition)
			if err != nil {
				t.Fatal(err)
			}
			a, _ = f.counts()
			if a != 2 {
				t.Fatal("stale launch adopted")
			}
			stopTestProcess(t, replacement)
		})
	}
}
func TestKubernetesRecoveryChecksRevisionAndActualSpec(t *testing.T) {
	for _, change := range []string{"revision", "arguments", "environment", "limits", "actual-image", "actual-command"} {
		t.Run(change, func(t *testing.T) {
			f := &runtimeFake{}
			d := testKube(t, f)
			s := validSpec()
			s.Driver = DriverKubernetes
			definition := recoveryDefinition()
			_, err := d.Start(t.Context(), s, definition)
			if err != nil {
				t.Fatal(err)
			}
			adopted, err := testKube(t, f).Start(t.Context(), s, definition)
			if err != nil {
				t.Fatal(err)
			}
			_ = adopted
			_, count := f.counts()
			if count != 1 {
				t.Fatal("matching pod was not adopted")
			}
			switch change {
			case "revision":
				s.Revision++
			case "arguments":
				definition.launch.Arguments = []string{"different"}
			case "environment":
				definition.launch.Environment = []string{"MODE=two"}
			case "limits":
				definition.launch.Limits.MilliCPU = 500
			case "actual-image", "actual-command":
				f.change(func() {
					c := f.pod["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
					if change == "actual-image" {
						c["image"] = "tampered"
					} else {
						c["command"] = []any{"different"}
					}
				})
			}
			replacement, err := testKube(t, f).Start(t.Context(), s, definition)
			if err != nil {
				t.Fatal(err)
			}
			_, count = f.counts()
			if count != 2 {
				t.Fatal("stale pod adopted")
			}
			stopTestProcess(t, replacement)
		})
	}
}
func TestPersistentDriversRefuseForeignOwnership(t *testing.T) {
	for _, kind := range []string{DriverDocker, DriverKubernetes} {
		t.Run(kind, func(t *testing.T) {
			f := &runtimeFake{}
			var d Driver
			if kind == DriverDocker {
				d = testDocker(t, f)
			} else {
				d = testKube(t, f)
			}
			s := validSpec()
			s.Driver = kind
			_, err := d.Start(t.Context(), s, recoveryDefinition())
			if err != nil {
				t.Fatal(err)
			}
			f.change(func() {
				if kind == DriverDocker {
					f.container.Config.Labels["workload.provisioning.node"] = "foreign"
				} else {
					f.pod["metadata"].(map[string]any)["labels"].(map[string]any)["workload.webong.dev/node"] = "foreign"
				}
			})
			if _, err = d.Start(t.Context(), s, recoveryDefinition()); err == nil {
				t.Fatal("adopted foreign resource")
			}
			f.change(func() {
				if kind == DriverDocker {
					f.container.Config.Labels["workload.provisioning.node"] = runtimeLabelValue("node-a")
				} else {
					f.pod["metadata"].(map[string]any)["labels"].(map[string]any)["workload.webong.dev/node"] = runtimeLabelValue("node-a")
				}
			})
			p, err := d.Start(t.Context(), s, recoveryDefinition())
			if err != nil {
				t.Fatal(err)
			}
			stopTestProcess(t, p)
		})
	}
}
func TestStopFailuresDoNotConfirmTermination(t *testing.T) {
	for _, kind := range []string{DriverDocker, DriverKubernetes} {
		t.Run(kind, func(t *testing.T) {
			f := &runtimeFake{}
			var d Driver
			if kind == DriverDocker {
				d = testDocker(t, f)
			} else {
				d = testKube(t, f)
			}
			s := validSpec()
			s.Driver = kind
			p, err := d.Start(t.Context(), s, recoveryDefinition())
			if err != nil {
				t.Fatal(err)
			}
			f.change(func() { f.failStop = true })
			if err = p.Stop(t.Context()); err == nil {
				t.Fatal("stop unexpectedly succeeded")
			}
			select {
			case <-p.Done():
				t.Fatal("failed stop closed Done")
			default:
			}
			f.change(func() { f.failStop = false })
			stopTestProcess(t, p)
		})
	}
}
func TestKubernetesDeleteUsesAtomicUIDPrecondition(t *testing.T) {
	f := &runtimeFake{}
	d := testKube(t, f)
	s := validSpec()
	s.Driver = DriverKubernetes
	p, err := d.Start(t.Context(), s, recoveryDefinition())
	if err != nil {
		t.Fatal(err)
	}
	f.change(func() { f.replaceOnDelete = true })
	if err = p.Stop(t.Context()); err == nil || !strings.Contains(err.Error(), "precondition") {
		t.Fatalf("expected UID rejection: %v", err)
	}
	select {
	case <-p.Done():
		t.Fatal("unconfirmed replacement closed Done")
	default:
	}
	// Restore the original identity solely to clean up this test's fake.
	f.change(func() { f.replaceOnDelete = false; f.pod["metadata"].(map[string]any)["uid"] = "pod-1" })
	stopTestProcess(t, p)
}
func TestKubernetesWaitsForDeletionConfirmation(t *testing.T) {
	f := &runtimeFake{}
	d := testKube(t, f)
	s := validSpec()
	s.Driver = DriverKubernetes
	p, err := d.Start(t.Context(), s, recoveryDefinition())
	if err != nil {
		t.Fatal(err)
	}
	f.change(func() { f.holdDelete = true })
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err = p.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline: %v", err)
	}
	select {
	case <-p.Done():
		t.Fatal("deletion acceptance is not termination")
	default:
	}
	f.change(func() { f.holdDelete = false })
	stopTestProcess(t, p)
}
func TestUncertainCreationRetainsCleanupHandle(t *testing.T) {
	for _, kind := range []string{DriverDocker, DriverKubernetes} {
		t.Run(kind, func(t *testing.T) {
			f := &runtimeFake{loseCreate: true}
			var d Driver
			if kind == DriverDocker {
				d = testDocker(t, f)
			} else {
				d = testKube(t, f)
			}
			s := validSpec()
			s.Driver = kind
			p, err := d.Start(t.Context(), s, recoveryDefinition())
			if err == nil || p == nil {
				t.Fatal("uncertain creation lost ownership handle")
			}
			stopTestProcess(t, p)
		})
	}
}
func TestNamedEndpointsAreRecovered(t *testing.T) {
	listeners := []net.Listener{}
	ports := []int{}
	for range 2 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
		defer l.Close()
	}
	_ = listeners
	for _, kind := range []string{DriverDocker, DriverKubernetes} {
		t.Run(kind, func(t *testing.T) {
			f := &runtimeFake{}
			var d Driver
			if kind == DriverDocker {
				d = testDocker(t, f)
			} else {
				d = testKube(t, f)
			}
			s := validSpec()
			s.Driver = kind
			def := recoveryDefinition()
			def.launch.Endpoints = []EndpointSpec{{"http", ports[0]}, {"metrics", ports[1]}}
			_, err := d.Start(t.Context(), s, def)
			if err != nil {
				t.Fatal(err)
			}
			p, err := d.Start(t.Context(), s, def)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Endpoints()) != 2 || p.Endpoints()[1].Name != "metrics" || p.Endpoints()[1].Port != ports[1] {
				t.Fatalf("lost named endpoints: %v", p.Endpoints())
			}
			stopTestProcess(t, p)
		})
	}
}
func TestResourceNamesPreserveExactIdentities(t *testing.T) {
	seen := map[string]bool{}
	for _, id := range []string{"Node A", "node-a", "node_a", strings.Repeat("x", 100) + "a", strings.Repeat("x", 100) + "b"} {
		name := runtimeResourceName(id)
		if seen[name] || len(name) > 63 {
			t.Fatal("identity collision or invalid name")
		}
		seen[name] = true
	}
}
func TestStaleCleanupOnlyDeletesOwnedResources(t *testing.T) {
	for _, kind := range []string{DriverDocker, DriverKubernetes} {
		t.Run(kind, func(t *testing.T) {
			f := &runtimeFake{}
			var d interface {
				Driver
				PrepareDesired(context.Context, map[string]Spec) error
			}
			if kind == DriverDocker {
				d = testDocker(t, f)
			} else {
				d = testKube(t, f)
			}
			s := validSpec()
			s.Driver = kind
			_, err := d.Start(t.Context(), s, recoveryDefinition())
			if err != nil {
				t.Fatal(err)
			}
			if err = d.PrepareDesired(t.Context(), map[string]Spec{s.AssignmentID: s}); err != nil {
				t.Fatal(err)
			}
			f.change(func() {
				if kind == DriverDocker {
					f.container.Config.Labels["workload.provisioning.node"] = "foreign"
				} else {
					f.pod["metadata"].(map[string]any)["labels"].(map[string]any)["workload.webong.dev/node"] = "foreign"
				}
			})
			if err = d.PrepareDesired(t.Context(), nil); err != nil {
				t.Fatal(err)
			}
			f.change(func() {
				if kind == DriverDocker {
					if f.container == nil {
						t.Fatal("deleted foreign container")
					}
					f.container.Config.Labels["workload.provisioning.node"] = runtimeLabelValue("node-a")
				} else {
					if f.pod == nil {
						t.Fatal("deleted foreign pod")
					}
					f.pod["metadata"].(map[string]any)["labels"].(map[string]any)["workload.webong.dev/node"] = runtimeLabelValue("node-a")
				}
			})
			if err = d.PrepareDesired(t.Context(), nil); err != nil {
				t.Fatal(err)
			}
			f.change(func() {
				if kind == DriverDocker && f.container != nil {
					t.Fatal("stale container retained")
				}
				if kind == DriverKubernetes && f.pod != nil {
					t.Fatal("stale pod retained")
				}
			})
		})
	}
}

func TestResourceQuantitiesSurviveKubernetesCanonicalization(t *testing.T) {
	f := &runtimeFake{}
	d := testKube(t, f)
	s := validSpec()
	s.Driver = DriverKubernetes
	def := recoveryDefinition()
	def.launch.Limits = ResourceLimits{MemoryBytes: 1 << 20, MilliCPU: 1000}
	_, err := d.Start(t.Context(), s, def)
	if err != nil {
		t.Fatal(err)
	}
	f.change(func() {
		container := f.pod["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
		limits := container["resources"].(map[string]any)["limits"].(map[string]any)
		limits["memory"] = "1Mi"
		limits["cpu"] = "1"
	})
	p, err := d.Start(t.Context(), s, def)
	if err != nil {
		t.Fatal(err)
	}
	_, count := f.counts()
	if count != 1 {
		t.Fatal("canonicalized quantities caused replacement")
	}
	stopTestProcess(t, p)
}
