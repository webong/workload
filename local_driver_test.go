//go:build linux || darwin

package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// These helpers run in different OS processes so worker exit cannot accidentally
// clean up a workload through a deferred Go function in the parent test.
func TestLocalTargetHelper(t *testing.T) {
	if os.Getenv("WORKLOAD_TARGET_HELPER") != "1" {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	l, err := net.Listen("tcp", os.Getenv("WORKLOAD_LISTEN"))
	if err != nil {
		os.Exit(2)
	}
	for {
		c, err := l.Accept()
		if err != nil {
			os.Exit(3)
		}
		_ = c.Close()
	}
}
func TestLocalWorkerHelper(t *testing.T) {
	if os.Getenv("WORKLOAD_WORKER_HELPER") != "1" {
		return
	}
	var c LocalDriverConfig
	if json.Unmarshal([]byte(os.Getenv("WORKLOAD_LOCAL_CONFIG")), &c) != nil {
		os.Exit(2)
	}
	d, err := NewLocalDriver(c)
	if err != nil {
		os.Exit(3)
	}
	s := validSpec()
	s.AssignmentID = "local-recovery"
	p, err := d.Start(context.Background(), s, &localDefinition{directory: c.StateDirectory})
	if err != nil {
		os.Exit(4)
	}
	if os.WriteFile(filepath.Join(c.StateDirectory, "worker-pid"), []byte(strconv.Itoa(p.PID())), 0600) != nil {
		os.Exit(5)
	}
	os.Exit(0) // Intentional abrupt worker exit, with no workload shutdown.
}

type localDefinition struct {
	directory string
	revision  string
}

func (*localDefinition) Type() string                         { return "test" }
func (*localDefinition) Supports(s string) bool               { return s == DriverLocal }
func (*localDefinition) Validate(context.Context, Spec) error { return nil }
func (d *localDefinition) Prepare(_ context.Context, _ Spec, p Placement) (LaunchSpec, error) {
	host := p.BindHost
	port := p.Ports["default"]
	return LaunchSpec{Executable: os.Args[0], Arguments: []string{"-test.run=^TestLocalTargetHelper$"}, Environment: []string{"WORKLOAD_TARGET_HELPER=1", "WORKLOAD_LISTEN=" + net.JoinHostPort(host, strconv.Itoa(port)), "REVISION=" + d.revision}, WorkingDirectory: d.directory, Port: port}, nil
}
func buildSupervisor(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "workload-supervisor")
	cmd := exec.Command("go", "build", "-o", path, "./cmd/workload-supervisor")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build supervisor: %v\n%s", err, out)
	}
	return path
}
func TestLocalSupervisorAdoptsAfterWorkerExitAndReplacesChangedLaunch(t *testing.T) {
	t.Setenv("TMPDIR", "/tmp")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	config := LocalDriverConfig{NodeID: "node", StateDirectory: dir, SupervisorBinary: buildSupervisor(t), StartTimeout: 5 * time.Second, StopTimeout: 2 * time.Second}
	b, _ := json.Marshal(config)
	worker := exec.Command(os.Args[0], "-test.run=^TestLocalWorkerHelper$")
	worker.Env = append(os.Environ(), "WORKLOAD_WORKER_HELPER=1", "WORKLOAD_LOCAL_CONFIG="+string(b))
	if out, err := worker.CombinedOutput(); err != nil {
		t.Fatalf("worker: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "worker-pid"))
	if err != nil {
		t.Fatal(err)
	}
	originalPID, err := strconv.Atoi(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewLocalDriver(config)
	if err != nil {
		t.Fatal(err)
	}
	s := validSpec()
	s.AssignmentID = "local-recovery"
	p, err := d.Start(t.Context(), s, &localDefinition{directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = p.Stop(ctx)
	})
	if p.PID() != originalPID {
		t.Fatalf("launched duplicate after worker exit: got %d want %d", p.PID(), originalPID)
	}
	// A separate driver instance can reattach to the same supervisor.
	again, err := NewLocalDriver(config)
	if err != nil {
		t.Fatal(err)
	}
	adopted, err := again.Start(t.Context(), s, &localDefinition{directory: dir})
	if err != nil {
		t.Fatal(err)
	}
	if adopted.PID() != originalPID {
		t.Fatal("reattach changed PID")
	}
	// Changed launch environment with unchanged revision must still replace.
	replacement, err := again.Start(t.Context(), s, &localDefinition{directory: dir, revision: "changed"})
	if err != nil {
		t.Fatal(err)
	}
	p = replacement
	if p.PID() == originalPID {
		t.Fatal("changed launch was adopted")
	}
	// The child ignores TERM; the supervisor must kill and reap it within budget.
	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err = p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2500*time.Millisecond {
		t.Fatal("local shutdown exceeded budget")
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("stop returned without confirmed child exit")
	}
	// The successful RPC reply may be lost; a later Stop still confirms the
	// durable terminal record without signaling any numeric PID.
	time.Sleep(30 * time.Millisecond)
	if err = p.Stop(t.Context()); err != nil {
		t.Fatalf("shutdown receipt was not recoverable: %v", err)
	}
}
func TestLocalOrphanJournalBlocksRelaunch(t *testing.T) {
	t.Setenv("TMPDIR", "/tmp")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	key := identityHash("orphan")
	if err := os.WriteFile(filepath.Join(dir, key+".json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ServeLocalSupervisor(dir, key, time.Second); err == nil || !strings.Contains(err.Error(), "orphaned") {
		t.Fatalf("orphaned journal permitted duplicate: %v", err)
	}
}

// Exercise replacement through the driver when the supervisor persists its stop
// receipt but the RPC reply is lost. A missing socket alone cannot prove exit.
func TestLocalReplacementRecoversLostStopReply(t *testing.T) {
	for _, lostReply := range []bool{false, true} {
		name := "socket already closed"
		if lostReply {
			name = "stop reply lost"
		}
		t.Run(name, func(t *testing.T) { testLocalReplacementReceipt(t, lostReply) })
	}
}

func testLocalReplacementReceipt(t *testing.T, lostReply bool) {
	t.Helper()
	t.Setenv("TMPDIR", "/tmp")
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	config := LocalDriverConfig{NodeID: "node", StateDirectory: dir, SupervisorBinary: buildSupervisor(t), StartTimeout: 5 * time.Second, StopTimeout: 2 * time.Second}
	driver, err := NewLocalDriver(config)
	if err != nil {
		t.Fatal(err)
	}
	spec := validSpec()
	spec.AssignmentID = "lost-replacement-reply"
	key := identityKey(config.NodeID, spec.TargetID, spec.AssignmentID)
	socket, err := localSocketPath(dir, key)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	recordPath := filepath.Join(dir, key+".json")
	record := localRecord{NodeID: config.NodeID, AssignmentID: spec.AssignmentID, Socket: socket, Fingerprint: "original-launch"}
	if err := writeLocalRecord(recordPath, record); err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				served <- err
				return
			}
			var request localRequest
			err = json.NewDecoder(conn).Decode(&request)
			if err == nil && request.Action == "status" {
				if !lostReply {
					// Retire the listener before returning its last status so the
					// subsequent Stop deterministically finds no socket.
					record.Done = true
					err = writeLocalRecord(recordPath, record)
					listener.Close()
				}
				if err == nil {
					err = json.NewEncoder(conn).Encode(localResponse{Started: true, Fingerprint: record.Fingerprint})
				}
				conn.Close()
				if err != nil || !lostReply {
					served <- err
					return
				}
				continue
			}
			if err == nil && request.Action != "stop" {
				err = fmt.Errorf("unexpected action %q", request.Action)
			}
			if err == nil && request.Fingerprint != record.Fingerprint {
				err = fmt.Errorf("stop did not identify the old launch: %q", request.Fingerprint)
			}
			if err == nil {
				record.Done = true
				err = writeLocalRecord(recordPath, record)
			}
			// Close both the socket and connection without delivering the reply.
			listener.Close()
			conn.Close()
			served <- err
			return
		}
	}()
	process, startErr := driver.Start(t.Context(), spec, &localDefinition{directory: dir})
	if process != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := process.Stop(ctx); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		})
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if startErr != nil {
		t.Fatalf("replace after confirmed shutdown with lost reply: %v", startErr)
	}
}

func TestLocalStopRequiresMatchingTerminalReceipt(t *testing.T) {
	for _, test := range []struct {
		name      string
		record    localRecord
		confirmed bool
	}{
		{"matching", localRecord{Fingerprint: "expected", Done: true}, true},
		{"different launch", localRecord{Fingerprint: "newer", Done: true}, false},
		{"not terminated", localRecord{Fingerprint: "expected"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "receipt.json")
			if err := writeLocalRecord(path, test.record); err != nil {
				t.Fatal(err)
			}
			process := &supervisedProcess{socket: filepath.Join(dir, "absent.sock"), recordPath: path, fingerprint: "expected", timeout: time.Second, done: make(chan struct{})}
			err := process.Stop(t.Context())
			if (err == nil) != test.confirmed {
				t.Fatalf("confirmed=%v: %v", test.confirmed, err)
			}
			select {
			case <-process.Done():
				if !test.confirmed {
					t.Fatal("unconfirmed process marked done")
				}
			default:
				if test.confirmed {
					t.Fatal("confirmed process not marked done")
				}
			}
		})
	}
}
