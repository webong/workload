//go:build linux || darwin

package workload

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func runningForward(t *testing.T) *localProcess {
	t.Helper()
	command := exec.Command("sleep", "60")
	configureProcessGroup(command)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	forward := &localProcess{command: command, done: make(chan struct{})}
	go forward.wait()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = forward.Stop(ctx)
	})
	return forward
}
func TestKubernetesStaleStopReleasesOwnedForwarders(t *testing.T) {
	forward := runningForward(t)
	runtime := &runtimeFake{pod: map[string]any{"metadata": map[string]any{"name": "example", "uid": "replacement"}, "status": map[string]any{"phase": "Running"}}}
	var process Process = &kubernetesProcess{runner: runtime, binary: "kubectl", namespace: "test", name: "example", uid: "original", forwards: []*localProcess{forward}, done: make(chan struct{}), stopTimeout: time.Second}
	if err := process.Stop(t.Context()); err == nil || !strings.Contains(err.Error(), "changed identity") {
		t.Fatalf("expected stale identity rejection: %v", err)
	}
	select {
	case <-forward.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("stale Pod handle leaked its owned port-forward")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	for _, call := range runtime.calls {
		if strings.Contains(call, "delete") {
			t.Fatal("stale handle attempted to delete the replacement")
		}
	}
}
func TestKubernetesObservedRemovalReleasesOwnedForwarders(t *testing.T) {
	forward := runningForward(t)
	process := &kubernetesProcess{runner: &runtimeFake{}, binary: "kubectl", namespace: "test", name: "example", uid: "original", forwards: []*localProcess{forward}, done: make(chan struct{}), stopTimeout: time.Second}
	go process.watch()
	select {
	case <-process.Done():
	case <-time.After(time.Second):
		t.Fatal("removal was not observed")
	}
	select {
	case <-forward.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("observed Pod removal leaked a port-forward")
	}
}
