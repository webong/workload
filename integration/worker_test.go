//go:build linux || darwin

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/webong/workload"
)

type workerConfig struct {
	Spec       workload.Spec
	Image      string
	Docker     workload.DockerDriverConfig
	Kubernetes workload.KubernetesDriverConfig
}

func (c workerConfig) driver() (recoveringDriver, error) {
	if c.Spec.Driver == workload.DriverDocker {
		return workload.NewDockerDriver(c.Docker)
	}
	return workload.NewKubernetesDriver(c.Kubernetes)
}
func (c workerConfig) node() string {
	if c.Spec.Driver == workload.DriverDocker {
		return c.Docker.NodeID
	}
	return c.Kubernetes.NodeID
}

type workerReport struct {
	Instances []workload.InstanceReport
	Error     string
}

func writeWorkerReport(path string, report workerReport) error {
	data, err := json.Marshal(report)
	if err != nil {
		return err
	}
	if err = os.WriteFile(path+".tmp", data, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}

// The helper is an actual separate worker process. Killing it bypasses all
// deferred shutdown and recovery is performed by a fresh Manager in a new PID.
func TestRuntimeWorkerHelper(t *testing.T) {
	if os.Getenv("WORKLOAD_TEST_WORKER") != "1" {
		return
	}
	var c workerConfig
	if err := json.Unmarshal([]byte(os.Getenv("WORKLOAD_TEST_WORKER_CONFIG")), &c); err != nil {
		t.Fatal(err)
	}
	path := requiredEnv(t, "WORKLOAD_TEST_WORKER_REPORT")
	driver, err := c.driver()
	if err != nil {
		t.Fatal(err)
	}
	m, err := workload.NewManager(workload.Options{NodeID: c.node(), Definitions: []workload.Definition{fixtureDefinition{c.Image}}, Drivers: map[string]workload.Driver{c.Spec.Driver: driver}, PollInterval: 500 * time.Millisecond, StartTimeout: 90 * time.Second, StopTimeout: 60 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = m.Apply(ctx, workload.Snapshot{Assignments: []workload.Spec{c.Spec}}); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() && scanner.Text() == "shutdown" {
			cancel()
		}
	}()
	result := make(chan error, 1)
	go func() { result <- m.Run(ctx) }()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-result:
			report := workerReport{Instances: m.Status()}
			if err != nil {
				report.Error = err.Error()
			}
			if writeErr := writeWorkerReport(path, report); writeErr != nil {
				t.Fatal(writeErr)
			}
			if err != nil {
				t.Fatal(err)
			}
			return
		case <-ticker.C:
			if err := writeWorkerReport(path, workerReport{Instances: m.Status()}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}
func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

type runtimeWorker struct {
	command    *exec.Cmd
	stdin      io.WriteCloser
	done       chan struct{}
	err        error
	log        *synchronizedBuffer
	reportPath string
}

func startRuntimeWorker(t *testing.T, c workerConfig) *runtimeWorker {
	t.Helper()
	encoded, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	w := &runtimeWorker{command: exec.Command(os.Args[0], "-test.run=^TestRuntimeWorkerHelper$"), done: make(chan struct{}), log: &synchronizedBuffer{}, reportPath: filepath.Join(t.TempDir(), "worker.json")}
	w.command.Env = append(os.Environ(), "WORKLOAD_TEST_WORKER=1", "WORKLOAD_TEST_WORKER_CONFIG="+string(encoded), "WORKLOAD_TEST_WORKER_REPORT="+w.reportPath)
	// kubectl port-forward subprocesses can outlive a hard-killed worker. Bound
	// pipe draining; the test's later Pod deletion terminates those forwarders.
	w.command.WaitDelay = 2 * time.Second
	w.command.Stdout = w.log
	w.command.Stderr = w.log
	w.stdin, err = w.command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = w.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { w.err = w.command.Wait(); close(w.done) }()
	t.Cleanup(func() { w.kill() })
	return w
}
func (w *runtimeWorker) kill() {
	select {
	case <-w.done:
		return
	default:
	}
	_ = w.command.Process.Kill()
	<-w.done
	_ = w.stdin.Close()
}
func (w *runtimeWorker) running(t *testing.T) workload.InstanceReport {
	t.Helper()
	deadline := time.NewTimer(150 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			t.Fatalf("worker exited before readiness: %v\n%s", w.err, w.log.String())
		case <-deadline.C:
			t.Fatalf("worker never became ready\n%s", w.log.String())
		case <-ticker.C:
			b, err := os.ReadFile(w.reportPath)
			if err != nil {
				continue
			}
			var report workerReport
			if json.Unmarshal(b, &report) != nil {
				continue
			}
			for _, instance := range report.Instances {
				if instance.State == "running" {
					return instance
				}
			}
		}
	}
}
func (w *runtimeWorker) shutdown(t *testing.T) {
	t.Helper()
	if _, err := fmt.Fprintln(w.stdin, "shutdown"); err != nil {
		t.Fatal(err)
	}
	_ = w.stdin.Close()
	select {
	case <-w.done:
		if w.err != nil {
			t.Fatalf("worker graceful shutdown: %v\n%s", w.err, w.log.String())
		}
	case <-time.After(90 * time.Second):
		t.Fatalf("worker shutdown timed out\n%s", w.log.String())
	}
	data, err := os.ReadFile(w.reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report workerReport
	if err = json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Error != "" || len(report.Instances) != 1 || report.Instances[0].State != "stopped" || len(report.Instances[0].Endpoints) != 0 {
		t.Fatalf("unclean final worker report: %+v", report)
	}
}
func workerCrashRecovery(t *testing.T, c workerConfig) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		d, err := c.driver()
		if err == nil {
			err = d.PrepareDesired(ctx, nil)
		}
		if err != nil {
			t.Errorf("cleanup crashed-worker resources: %v", err)
		}
	})
	first := startRuntimeWorker(t, c)
	before := first.running(t)
	identityBefore := readEndpointIdentities(t, before.Endpoints)
	first.kill()
	if first.err == nil {
		t.Fatal("worker was not hard-killed")
	}
	// Docker publishes the data-plane port independently of its worker. A
	// Kubernetes port-forward is worker-owned, so verify the Pod via its API and
	// then compare HTTP boot IDs through the recovered worker's new forwarders.
	if c.Spec.Driver == workload.DriverDocker {
		if id := readEndpointIdentities(t, before.Endpoints); id != identityBefore {
			t.Fatal("workload changed when worker crashed")
		}
	} else {
		phase := kubectl(t, c.Kubernetes.Kubeconfig, "get", "pod", identityBefore.Hostname, "--namespace", c.Kubernetes.Namespace, "-o", "jsonpath={.status.phase}")
		if phase != "Running" {
			t.Fatalf("Pod did not survive worker crash: %s", phase)
		}
	}
	second := startRuntimeWorker(t, c)
	after := second.running(t)
	identityAfter := readEndpointIdentities(t, after.Endpoints)
	if identityAfter != identityBefore {
		t.Fatalf("fresh worker recreated workload: before=%+v after=%+v", identityBefore, identityAfter)
	}
	second.shutdown(t)
}
func TestDockerIntegrationWorkerRecoversAfterCrash(t *testing.T) {
	if os.Getenv("WORKLOAD_TEST_DOCKER") != "1" {
		t.Skip("run scripts/integration-test.sh docker")
	}
	c := dockerConfig(t)
	s := assignment(workload.DriverDocker)
	s.TargetID = c.TargetID
	workerCrashRecovery(t, workerConfig{Spec: s, Image: requiredEnv(t, "WORKLOAD_TEST_IMAGE"), Docker: c})
}
func TestKubernetesIntegrationWorkerRecoversAfterCrash(t *testing.T) {
	if os.Getenv("WORKLOAD_TEST_KUBERNETES") != "1" {
		t.Skip("run scripts/integration-test.sh kubernetes")
	}
	c := kubernetesConfig(t)
	s := assignment(workload.DriverKubernetes)
	s.TargetID = c.TargetID
	workerCrashRecovery(t, workerConfig{Spec: s, Image: requiredEnv(t, "WORKLOAD_TEST_IMAGE"), Kubernetes: c})
}
