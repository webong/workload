package workload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DockerDriverConfig struct {
	TargetID, Context, Binary, NodeID, Network, EnvironmentFile, PublishHost, RouteHost string
	StartTimeout, StopTimeout                                                           time.Duration
}
type DockerDriver struct {
	config DockerDriverConfig
	runner commandRunner
}

func NewDockerDriver(c DockerDriverConfig) (*DockerDriver, error) {
	return newDockerDriver(c, execCommandRunner{})
}
func newDockerDriver(c DockerDriverConfig, r commandRunner) (*DockerDriver, error) {
	if c.Binary == "" || c.NodeID == "" || c.PublishHost == "" || c.StartTimeout <= 0 || c.StopTimeout <= 0 || r == nil {
		return nil, fmt.Errorf("Docker binary, node, publish host, positive timeouts and runner are required")
	}
	if c.Context != "" {
		r = prefixedCommandRunner{r, []string{"--context", c.Context}}
	}
	return &DockerDriver{c, r}, nil
}

type dockerInspection struct {
	ID     string `json:"Id"`
	Name   string `json:"Name"`
	Config struct {
		Image      string            `json:"Image"`
		Labels     map[string]string `json:"Labels"`
		Entrypoint []string          `json:"Entrypoint"`
		Cmd        []string          `json:"Cmd"`
		Env        []string          `json:"Env"`
		WorkingDir string            `json:"WorkingDir"`
	} `json:"Config"`
	HostConfig struct {
		Memory      int64  `json:"Memory"`
		NanoCpus    int64  `json:"NanoCpus"`
		PidsLimit   *int64 `json:"PidsLimit"`
		NetworkMode string `json:"NetworkMode"`
	} `json:"HostConfig"`
	State struct {
		Running  bool `json:"Running"`
		Pid      int  `json:"Pid"`
		ExitCode int  `json:"ExitCode"`
	} `json:"State"`
	NetworkSettings struct {
		Ports map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
	} `json:"NetworkSettings"`
}

func (d *DockerDriver) inspect(ctx context.Context, id string) (dockerInspection, error) {
	out, err := d.runner.Output(ctx, nil, d.config.Binary, "inspect", "--format", "{{json .}}", id)
	if err != nil {
		return dockerInspection{}, err
	}
	var v dockerInspection
	err = json.Unmarshal([]byte(out), &v)
	return v, err
}
func (d *DockerDriver) owns(labels map[string]string) bool {
	return labels["workload.provisioning.node"] == runtimeLabelValue(d.config.NodeID) && labels["workload.provisioning.target"] == runtimeLabelValue(d.config.TargetID)
}
func (d *DockerDriver) PrepareDesired(ctx context.Context, desired map[string]Spec) error {
	out, err := d.runner.Output(ctx, nil, d.config.Binary, "ps", "--all", "--quiet", "--no-trunc", "--filter", "label=workload.provisioning.node="+runtimeLabelValue(d.config.NodeID), "--filter", "label=workload.provisioning.target="+runtimeLabelValue(d.config.TargetID))
	if err != nil {
		return err
	}
	for _, id := range strings.Fields(out) {
		v, err := d.inspect(ctx, id)
		if err != nil {
			return err
		}
		if !d.owns(v.Config.Labels) {
			continue
		}
		assignment := v.Config.Labels["workload.provisioning.instance"]
		if _, ok := desired[assignment]; ok {
			continue
		} // Start verifies revision and launch before adoption.
		if err := d.remove(ctx, id); err != nil {
			return fmt.Errorf("remove stale container %s: %w", id, err)
		}
	}
	return nil
}
func (d *DockerDriver) remove(ctx context.Context, id string) error {
	_, err := d.runner.Output(ctx, nil, d.config.Binary, "rm", "--force", id)
	return err
}
func (d *DockerDriver) Start(ctx context.Context, s Spec, definition Definition) (Process, error) {
	id := s.AssignmentID
	ctx, cancel := context.WithTimeout(ctx, d.config.StartTimeout)
	defer cancel()
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if s.Driver != DriverDocker {
		return nil, fmt.Errorf("Docker cannot run driver %q", s.Driver)
	}
	launch, err := definition.Prepare(ctx, s, Placement{Driver: DriverDocker, BindHost: "0.0.0.0"})
	if err != nil {
		return nil, err
	}
	if err = launch.validate(true); err != nil {
		return nil, err
	}
	// The environment file contents, not merely its path, participate in identity.
	fingerprint, err := dockerFingerprint(d.config, s, launch)
	if err != nil {
		return nil, err
	}
	name := runtimeResourceName(identityKey(d.config.NodeID, d.config.TargetID, id))
	existing, err := d.runner.Output(ctx, nil, d.config.Binary, "ps", "--all", "--quiet", "--no-trunc", "--filter", "name=^/"+name+"$")
	if err != nil {
		return nil, err
	}
	var v dockerInspection
	if strings.TrimSpace(existing) != "" {
		ids := strings.Fields(existing)
		if len(ids) != 1 {
			return nil, fmt.Errorf("multiple containers match assignment")
		}
		v, err = d.inspect(ctx, ids[0])
		if err != nil {
			return nil, err
		}
		if v.ID != ids[0] || v.Name != "/"+name || !d.owns(v.Config.Labels) || v.Config.Labels["workload.provisioning.instance"] != id {
			return nil, fmt.Errorf("container ownership conflict for %s", id)
		}
		if v.Config.Labels["workload.provisioning.fingerprint"] != fingerprint || !v.State.Running || !dockerLaunchMatches(v, launch, d.config) {
			if err = d.remove(ctx, v.ID); err != nil {
				return nil, fmt.Errorf("replace stale container: %w", err)
			}
			v = dockerInspection{}
		}
	}
	if v.ID == "" {
		args, argsErr := dockerRunArguments(d.config, s, id, name, launch)
		if argsErr != nil {
			return nil, argsErr
		}
		// dockerRunArguments also includes the fingerprint; propagate file read errors above.
		out, err := d.runner.Output(ctx, nil, d.config.Binary, args...)
		if err != nil {
			return d.pending(name, id, fingerprint), err
		}
		containerID := dockerContainerID(out)
		if containerID == "" {
			return d.pending(name, id, fingerprint), fmt.Errorf("Docker returned an empty container ID")
		}
		v, err = d.inspect(ctx, containerID)
		if err != nil {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), d.config.StopTimeout)
			cleanupErr := d.remove(cleanupCtx, containerID)
			cleanupCancel()
			if cleanupErr != nil {
				return d.pending(name, id, fingerprint), errors.Join(err, cleanupErr)
			}
			return nil, err
		}
		if v.ID != containerID || !d.owns(v.Config.Labels) || v.Config.Labels["workload.provisioning.fingerprint"] != fingerprint {
			return d.pending(name, id, fingerprint), fmt.Errorf("created container identity does not match launch")
		}
	}
	endpoints := []Endpoint{}
	for _, e := range launch.endpoints() {
		bindings := v.NetworkSettings.Ports[strconv.Itoa(e.Port)+"/tcp"]
		if len(bindings) != 1 {
			return d.pending(name, id, fingerprint), fmt.Errorf("container %s has no unique endpoint %s", v.ID, e.Name)
		}
		port, err := strconv.Atoi(bindings[0].HostPort)
		if err != nil || port < 1 || port > 65535 {
			return d.pending(name, id, fingerprint), fmt.Errorf("invalid published endpoint %s", e.Name)
		}
		endpoints = append(endpoints, Endpoint{e.Name, dockerRouteHost(d.config.RouteHost, bindings[0].HostIP), port})
	}
	p := &dockerProcess{runner: d.runner, binary: d.config.Binary, containerID: v.ID, pid: v.State.Pid, endpoints: endpoints, stopTimeout: d.config.StopTimeout, done: make(chan struct{})}
	if len(endpoints) > 0 {
		p.host = endpoints[0].Host
		p.port = endpoints[0].Port
	}
	go p.watch()
	if err = waitForListener(ctx, p); err != nil {
		return p, err
	} // Return ownership even on readiness failure.
	return p, nil
}
func dockerContainerID(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}
func dockerRunArguments(c DockerDriverConfig, s Spec, id, name string, l LaunchSpec) ([]string, error) {
	fingerprint, err := dockerFingerprint(c, s, l)
	if err != nil {
		return nil, err
	}
	args := []string{"run", "--detach", "--name", name,
		"--label", "workload.provisioning.type=" + runtimeLabelValue(s.Type), "--label", "workload.provisioning.server=" + runtimeLabelValue(s.ID),
		"--label", "workload.provisioning.instance=" + id, "--label", "workload.provisioning.node=" + runtimeLabelValue(c.NodeID),
		"--label", "workload.provisioning.target=" + runtimeLabelValue(c.TargetID), "--label", "workload.provisioning.fingerprint=" + fingerprint}
	for _, e := range l.endpoints() {
		args = append(args, "--publish", net.JoinHostPort(c.PublishHost, "")+":"+strconv.Itoa(e.Port))
	}
	if c.Network != "" {
		args = append(args, "--network", c.Network)
	}
	if c.EnvironmentFile != "" {
		args = append(args, "--env-file", c.EnvironmentFile)
	}
	for _, v := range l.Environment {
		args = append(args, "--env", v)
	}
	if l.WorkingDirectory != "" {
		args = append(args, "--workdir", l.WorkingDirectory)
	}
	if l.Limits.MemoryBytes > 0 {
		args = append(args, "--memory", strconv.FormatInt(l.Limits.MemoryBytes, 10))
	}
	if l.Limits.MilliCPU > 0 {
		args = append(args, "--cpus", strconv.FormatFloat(float64(l.Limits.MilliCPU)/1000, 'f', 3, 64))
	}
	if l.Limits.PIDs > 0 {
		args = append(args, "--pids-limit", strconv.FormatInt(l.Limits.PIDs, 10))
	}
	args = append(args, "--entrypoint", l.Executable, l.Image)
	return append(args, l.Arguments...), nil
}
func dockerRouteHost(configured, published string) string {
	if configured != "" {
		return configured
	}
	if published == "0.0.0.0" || published == "::" || published == "" {
		return "127.0.0.1"
	}
	return published
}

type dockerProcess struct {
	runner                    commandRunner
	binary, containerID, host string
	port, pid                 int
	endpoints                 []Endpoint
	stopTimeout               time.Duration
	done                      chan struct{}
	doneOnce                  sync.Once
	errMu                     sync.RWMutex
	err                       error
	stopMu                    sync.Mutex
}

func (p *dockerProcess) inspect(ctx context.Context) (dockerInspection, bool, error) {
	out, err := p.runner.Output(ctx, nil, p.binary, "ps", "--all", "--quiet", "--no-trunc", "--filter", "id="+p.containerID)
	if err != nil {
		return dockerInspection{}, false, err
	}
	if strings.TrimSpace(out) == "" {
		return dockerInspection{}, true, nil
	}
	v, err := (&DockerDriver{config: DockerDriverConfig{Binary: p.binary}, runner: p.runner}).inspect(ctx, p.containerID)
	if err == nil && v.ID != p.containerID {
		return v, false, fmt.Errorf("container identity changed")
	}
	return v, false, err
}
func (p *dockerProcess) watch() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.timeout())
		v, absent, err := p.inspect(ctx)
		cancel()
		// Losing the daemon connection must never make a still-running container
		// appear terminated. Stop remains retryable and the assignment stays owned.
		if err != nil {
			continue
		}
		if absent {
			p.finish(nil)
			return
		}
		if !v.State.Running {
			if v.State.ExitCode != 0 {
				p.finish(fmt.Errorf("container exited with status %d", v.State.ExitCode))
			} else {
				p.finish(nil)
			}
			return
		}
	}
}
func (p *dockerProcess) timeout() time.Duration {
	if p.stopTimeout <= 0 {
		return time.Second
	}
	return p.stopTimeout
}
func (p *dockerProcess) finish(err error) {
	p.doneOnce.Do(func() { p.errMu.Lock(); p.err = err; p.errMu.Unlock(); close(p.done) })
}
func (p *dockerProcess) PID() int              { return p.pid }
func (p *dockerProcess) Host() string          { return p.host }
func (p *dockerProcess) Port() int             { return p.port }
func (p *dockerProcess) Done() <-chan struct{} { return p.done }
func (p *dockerProcess) Err() error            { p.errMu.RLock(); defer p.errMu.RUnlock(); return p.err }
func (p *dockerProcess) Endpoints() []Endpoint { return append([]Endpoint(nil), p.endpoints...) }
func (p *dockerProcess) Stop(ctx context.Context) error {
	p.stopMu.Lock()
	defer p.stopMu.Unlock()
	v, absent, err := p.inspect(ctx)
	if err != nil {
		return err
	}
	if absent {
		p.finish(nil)
		return nil
	}
	if v.State.Running {
		_, stopErr := p.runner.Output(ctx, nil, p.binary, "stop", "--time", strconv.Itoa(int(math.Ceil(p.timeout().Seconds()))), p.containerID)
		v, absent, err = p.inspect(ctx)
		if err != nil {
			return errors.Join(stopErr, err)
		}
		if !absent && v.State.Running {
			return errors.Join(stopErr, fmt.Errorf("container termination is unconfirmed"))
		}
	}
	if !absent {
		if _, err = p.runner.Output(ctx, nil, p.binary, "rm", p.containerID); err != nil {
			return err
		}
	}
	p.finish(nil)
	return nil
}

func (d *DockerDriver) pending(name, id, fingerprint string) Process {
	return pending(func(ctx context.Context) error {
		out, err := d.runner.Output(ctx, nil, d.config.Binary, "ps", "--all", "--quiet", "--no-trunc", "--filter", "name=^/"+name+"$")
		if err != nil {
			return err
		}
		ids := strings.Fields(out)
		if len(ids) != 1 {
			return fmt.Errorf("container creation outcome remains unknown")
		}
		v, err := d.inspect(ctx, ids[0])
		if err != nil {
			return err
		}
		if v.ID != ids[0] || v.Name != "/"+name || !d.owns(v.Config.Labels) || v.Config.Labels["workload.provisioning.instance"] != id || v.Config.Labels["workload.provisioning.fingerprint"] != fingerprint {
			return fmt.Errorf("pending container ownership conflict")
		}
		p := &dockerProcess{runner: d.runner, binary: d.config.Binary, containerID: v.ID, stopTimeout: d.config.StopTimeout, done: make(chan struct{})}
		return p.Stop(ctx)
	})
}

func dockerLaunchMatches(v dockerInspection, l LaunchSpec, c DockerDriverConfig) bool {
	if v.Config.Image != l.Image || !slices.Equal(v.Config.Entrypoint, []string{l.Executable}) || !slices.Equal(v.Config.Cmd, l.Arguments) {
		return false
	}
	if l.WorkingDirectory != "" && v.Config.WorkingDir != l.WorkingDirectory {
		return false
	}
	env := map[string]string{}
	for _, item := range v.Config.Env {
		k, value, _ := strings.Cut(item, "=")
		env[k] = value
	}
	for _, item := range l.Environment {
		k, value, _ := strings.Cut(item, "=")
		if actual, ok := env[k]; !ok || actual != value {
			return false
		}
	}
	if l.Limits.MemoryBytes > 0 && v.HostConfig.Memory != l.Limits.MemoryBytes {
		return false
	}
	if l.Limits.MilliCPU > 0 && (l.Limits.MilliCPU > 9223372036854 || v.HostConfig.NanoCpus != l.Limits.MilliCPU*1000000) {
		return false
	}
	if l.Limits.PIDs > 0 && (v.HostConfig.PidsLimit == nil || *v.HostConfig.PidsLimit != l.Limits.PIDs) {
		return false
	}
	if c.Network != "" && v.HostConfig.NetworkMode != c.Network {
		return false
	}
	return true
}
