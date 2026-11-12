//go:build linux || darwin

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu    sync.Mutex
	bytes bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bytes.Write(data)
}
func (b *lockedBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.bytes.String() }
func eventually(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(description)
}
func TestCompleteAppWithRealLocalWorkers(t *testing.T) {
	directory := t.TempDir()
	binary := filepath.Join(directory, "control-plane")
	supervisor := filepath.Join(directory, "workload-supervisor")
	for _, build := range []struct{ output, path string }{{binary, "."}, {supervisor, "../../cmd/workload-supervisor"}} {
		command := exec.Command("go", "build", "-o", build.output, build.path)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", build.path, err, output)
		}
	}
	// The executable uses port zero and prints the actual loopback listener. No
	// reservation-to-launch race or fixed CI port is needed for the host API.
	logs := &lockedBuffer{}
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(logs.String())
		}
	})
	command := exec.Command(binary, "-listen", "127.0.0.1:0", "-state-dir", filepath.Join(directory, "state"), "-supervisor", supervisor, "-lease-duration", "3s")
	command.Stdout = logs
	command.Stderr = logs
	command.WaitDelay = 2 * time.Second
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var commandErr error
	go func() { commandErr = command.Wait(); close(done) }()
	t.Cleanup(func() {
		select {
		case <-done:
			return
		default:
		}
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(12 * time.Second):
			_ = command.Process.Kill()
			<-done
			t.Errorf("app cleanup timed out\n%s", logs.String())
		}
	})
	var baseURL string
	eventually(t, "example API did not start", func() bool {
		for _, line := range strings.Split(logs.String(), "\n") {
			if value, ok := strings.CutPrefix(line, "control plane: "); ok {
				baseURL = value
				return true
			}
		}
		return false
	})
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	defer client.CloseIdleConnections()
	call := func(method, path, body string) {
		t.Helper()
		request, err := http.NewRequest(method, baseURL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode >= 300 {
			data, _ := io.ReadAll(response.Body)
			t.Fatalf("%s %s: %d %s", method, path, response.StatusCode, data)
		}
	}
	instances := func() []instanceView {
		response, err := client.Get(baseURL + "/instances")
		if err != nil {
			return nil
		}
		defer response.Body.Close()
		var result []instanceView
		_ = json.NewDecoder(response.Body).Decode(&result)
		return result
	}
	ready := func(count int, revision int64) bool {
		items := instances()
		found := 0
		for _, item := range items {
			if item.Ready {
				if item.Revision != revision {
					return false
				}
				found++
			}
		}
		return found == count
	}
	readMessage := func(item instanceView) string {
		t.Helper()
		if len(item.Endpoints) != 1 {
			t.Fatal("missing HTTP endpoint")
		}
		response, err := client.Get(item.Endpoints[0].URL)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var data struct {
			Message string
			PID     int
		}
		if err = json.NewDecoder(response.Body).Decode(&data); err != nil {
			t.Fatal(err)
		}
		if data.PID != item.PID {
			t.Fatal("reported endpoint does not reach the managed process")
		}
		return data.Message
	}

	call(http.MethodPut, "/workloads/hello", `{"message":"hello","replicas":2}`)
	eventually(t, "two real workers never became ready", func() bool { return ready(2, 1) })
	for _, item := range instances() {
		if item.Ready && readMessage(item) != "hello" {
			t.Fatal("wrong worker response")
		}
	}
	call(http.MethodPut, "/workloads/hello", `{"message":"updated","replicas":2}`)
	eventually(t, "configuration revision did not replace workers", func() bool { return ready(2, 2) })
	items := instances()
	var survivorPID int
	var removedAddress string
	for _, item := range items {
		if item.AssignmentID == "hello/0" {
			survivorPID = item.PID
		}
		if item.AssignmentID == "hello/1" {
			removedAddress = net.JoinHostPort(item.Endpoints[0].Host, fmt.Sprint(item.Endpoints[0].Port))
		}
		if item.Ready && readMessage(item) != "updated" {
			t.Fatal("new configuration was not applied")
		}
	}
	call(http.MethodPut, "/workloads/hello", `{"message":"updated","replicas":1}`)
	eventually(t, "scale-down did not stop the second replica", func() bool {
		for _, item := range instances() {
			if item.AssignmentID == "hello/1" {
				return item.State == "stopped"
			}
		}
		return false
	})
	for _, item := range instances() {
		if item.Ready && item.PID != survivorPID {
			t.Fatal("scaling restarted the retained replica")
		}
	}
	eventually(t, "removed replica still accepts connections", func() bool {
		connection, err := net.DialTimeout("tcp", removedAddress, 100*time.Millisecond)
		if err == nil {
			_ = connection.Close()
		}
		return err != nil
	})

	call(http.MethodPost, "/control-plane/pause", "")
	eventually(t, "lease expiry did not stop the remaining worker", func() bool {
		for _, item := range instances() {
			if item.AssignmentID == "hello/0" {
				return item.State == "stopped" && !item.Ready
			}
		}
		return false
	})
	call(http.MethodPost, "/control-plane/resume", "")
	eventually(t, "resume did not issue a fresh lease", func() bool { return ready(1, 2) })
	for _, item := range instances() {
		if item.Ready && item.PID == survivorPID {
			t.Fatal("lease expiry did not actually terminate the original process")
		}
	}
	call(http.MethodDelete, "/workloads/hello", "")
	eventually(t, "delete did not terminate the worker", func() bool {
		for _, item := range instances() {
			if item.AssignmentID == "hello/0" {
				return item.State == "stopped" && !item.Ready
			}
		}
		return false
	})
	// Shutdown must also clean up an active workload, independently of deletion.
	call(http.MethodPut, "/workloads/hello", `{"message":"shutdown","replicas":1}`)
	eventually(t, "shutdown worker never became ready", func() bool { return ready(1, 3) })
	var shutdownAddress string
	for _, item := range instances() {
		if item.Ready {
			shutdownAddress = net.JoinHostPort(item.Endpoints[0].Host, fmt.Sprint(item.Endpoints[0].Port))
		}
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		if commandErr != nil {
			t.Fatalf("example exited with unresolved shutdown: %v\n%s", commandErr, logs.String())
		}
	case <-time.After(12 * time.Second):
		t.Fatalf("example did not shut down\n%s", logs.String())
	}
	eventually(t, "shutdown left a worker accepting connections", func() bool {
		connection, err := net.DialTimeout("tcp", shutdownAddress, 100*time.Millisecond)
		if err == nil {
			connection.Close()
		}
		return err != nil
	})
}

func TestOnlyOneControllerOwnsStateDirectory(t *testing.T) {
	directory := t.TempDir()
	first, err := claimOwner(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := claimOwner(directory); err == nil {
		second.Close()
		t.Fatal("second controller acquired active state directory")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := claimOwner(directory)
	if err != nil {
		t.Fatal("ownership was not released:", err)
	}
	third.Close()
}
