package workload

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// LocalDriverConfig requires the companion supervisor executable. A supervisor
// owns each process group and remains reachable after its original worker exits.
type LocalDriverConfig struct {
	NodeID, StateDirectory, SupervisorBinary string
	StartTimeout, StopTimeout                time.Duration
}
type LocalDriver struct{ config LocalDriverConfig }

func NewLocalDriver(c LocalDriverConfig) (*LocalDriver, error) {
	if c.StartTimeout <= 0 || c.StopTimeout <= 0 || c.NodeID == "" || c.StateDirectory == "" || c.SupervisorBinary == "" {
		return nil, fmt.Errorf("local driver requires node, state directory, supervisor binary and positive timeouts")
	}
	dir, err := filepath.Abs(c.StateDirectory)
	if err != nil {
		return nil, err
	}
	c.StateDirectory = dir
	if err = privateDirectory(dir); err != nil {
		return nil, err
	}
	return &LocalDriver{c}, nil
}
func (d *LocalDriver) Start(ctx context.Context, s Spec, definition Definition) (Process, error) {
	id := s.AssignmentID
	ctx, cancel := context.WithTimeout(ctx, d.config.StartTimeout)
	defer cancel()
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if s.Driver != DriverLocal {
		return nil, fmt.Errorf("local driver cannot run %q", s.Driver)
	}
	key := identityKey(d.config.NodeID, s.TargetID, id)
	socket, err := localSocketPath(d.config.StateDirectory, key)
	if err != nil {
		return nil, err
	}
	status, statusErr := localCall(ctx, socket, localRequest{Action: "status"})

	host := "127.0.0.1"
	port := 0
	if len(status.Launch.endpoints()) > 0 {
		port = status.Launch.endpoints()[0].Port
	} else {
		port, err = availablePort(host)
		if err != nil {
			return nil, err
		}
	}
	ports := map[string]int{"default": port}
	for _, e := range status.Launch.endpoints() {
		ports[e.Name] = e.Port
	}
	launch, err := definition.Prepare(ctx, s, Placement{Driver: DriverLocal, BindHost: host, Ports: ports})
	if err != nil {
		return nil, err
	}
	if err = launch.validate(false); err != nil {
		return nil, err
	}
	if launch.Limits != (ResourceLimits{}) {
		return nil, fmt.Errorf("local resource limits require an OS-specific driver; refusing to ignore requested limits")
	}
	if launch.WorkingDirectory == "" {
		return nil, fmt.Errorf("local working directory is required")
	}
	fingerprint := launchFingerprint(s, launch, d.config)
	if statusErr != nil {
		// The supervisor's advisory lock, not socket existence, arbitrates concurrent
		// starts. Workers never remove an existing socket or signal a recorded PID.
		command := exec.Command(d.config.SupervisorBinary, "--state-dir", d.config.StateDirectory, "--key", key, "--stop-timeout", d.config.StopTimeout.String())
		if err = command.Start(); err != nil {
			return nil, fmt.Errorf("start local supervisor: %w", err)
		}
		go func() { _ = command.Wait() }()
		for {
			status, statusErr = localCall(ctx, socket, localRequest{Action: "status"})
			if statusErr == nil {
				break
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	if status.Started && status.Fingerprint != fingerprint {
		// Preserve the observed launch identity: the stop reply can be lost as
		// the old supervisor closes its socket. Only its matching durable receipt
		// may confirm termination, and the stop must not target a newer launch.
		p := &supervisedProcess{socket: socket, recordPath: filepath.Join(d.config.StateDirectory, key+".json"), fingerprint: status.Fingerprint, timeout: d.config.StopTimeout, done: make(chan struct{})}
		if err = p.Stop(ctx); err != nil {
			return p, fmt.Errorf("replace local process: %w", err)
		}
		// Stop closes the old supervisor. A fresh start recovers a new owner.
		return d.Start(ctx, s, definition)
	}
	response, err := localCall(ctx, socket, localRequest{Action: "start", Fingerprint: fingerprint, Launch: launch, NodeID: d.config.NodeID, AssignmentID: id})
	if err != nil {
		return &supervisedProcess{socket: socket, recordPath: filepath.Join(d.config.StateDirectory, key+".json"), fingerprint: fingerprint, timeout: d.config.StopTimeout, done: make(chan struct{})}, err
	}
	p := &supervisedProcess{socket: socket, recordPath: filepath.Join(d.config.StateDirectory, key+".json"), fingerprint: fingerprint, pid: response.PID, timeout: d.config.StopTimeout, done: make(chan struct{})}
	for _, e := range launch.endpoints() {
		p.endpoints = append(p.endpoints, Endpoint{e.Name, host, e.Port})
	}
	go p.watch()
	if err = waitForListener(ctx, p); err != nil {
		return p, err
	}
	return p, nil
}

// PrepareDesired contacts only supervisors in this driver's private state
// directory. Journal records contain the exact owner and assignment identity.
func (d *LocalDriver) PrepareDesired(ctx context.Context, desired map[string]Spec) error {
	records, err := filepath.Glob(filepath.Join(d.config.StateDirectory, "*.json"))
	if err != nil {
		return err
	}
	for _, path := range records {
		record, err := readLocalRecord(path)
		if err != nil {
			return err
		}
		if record.Done {
			continue
		}
		if record.NodeID != d.config.NodeID {
			continue
		}
		if _, ok := desired[record.AssignmentID]; ok {
			continue
		}
		p := &supervisedProcess{socket: record.Socket, recordPath: path, fingerprint: record.Fingerprint, timeout: d.config.StopTimeout, done: make(chan struct{})}
		if err = p.Stop(ctx); err != nil {
			return fmt.Errorf("stop stale local assignment %s: %w", record.AssignmentID, err)
		}
	}
	return nil
}

type supervisedProcess struct {
	socket, fingerprint string
	recordPath          string
	pid                 int
	endpoints           []Endpoint
	timeout             time.Duration
	done                chan struct{}
	once                sync.Once
	mu                  sync.RWMutex
	err                 error
}

func (p *supervisedProcess) PID() int { return p.pid }
func (p *supervisedProcess) Host() string {
	if len(p.endpoints) > 0 {
		return p.endpoints[0].Host
	}
	return ""
}
func (p *supervisedProcess) Port() int {
	if len(p.endpoints) > 0 {
		return p.endpoints[0].Port
	}
	return 0
}
func (p *supervisedProcess) Endpoints() []Endpoint { return append([]Endpoint(nil), p.endpoints...) }
func (p *supervisedProcess) Done() <-chan struct{} { return p.done }
func (p *supervisedProcess) Err() error            { p.mu.RLock(); defer p.mu.RUnlock(); return p.err }
func (p *supervisedProcess) finish(err error) {
	p.once.Do(func() { p.mu.Lock(); p.err = err; p.mu.Unlock(); close(p.done) })
}
func (p *supervisedProcess) watch() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
		s, err := localCall(ctx, p.socket, localRequest{Action: "status"})
		cancel()
		if err != nil {
			continue
		} // A missing supervisor is unconfirmed, never a dead process.
		if s.Fingerprint != p.fingerprint {
			continue
		}
		if s.Done {
			if s.ExitError != "" {
				p.finish(errors.New(s.ExitError))
			} else {
				p.finish(nil)
			}
			return
		}
	}
}
func (p *supervisedProcess) Stop(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	s, err := localCall(ctx, p.socket, localRequest{Action: "stop", Fingerprint: p.fingerprint})
	if err != nil {
		record, recordErr := readLocalRecord(p.recordPath)
		if recordErr == nil && record.Done && record.Fingerprint == p.fingerprint {
			p.finish(nil)
			return nil
		}
		return fmt.Errorf("local termination unconfirmed: %w", err)
	}
	if !s.Done {
		return fmt.Errorf("local supervisor did not confirm termination")
	}
	p.finish(nil)
	return nil
}

// localProcess is used only for a worker-owned kubectl port-forward. Managed
// local workloads use supervisedProcess so they can survive and be adopted.
type localProcess struct {
	command  *exec.Cmd
	host     string
	port     int
	done     chan struct{}
	errMu    sync.RWMutex
	err      error
	groupErr error
	stopOnce sync.Once
}

func (p *localProcess) wait() {
	err := p.command.Wait()
	groupErr := killProcessGroup(p.command)
	p.errMu.Lock()
	p.err = err
	p.groupErr = groupErr
	p.errMu.Unlock()
	close(p.done)
}
func (p *localProcess) PID() int              { return p.command.Process.Pid }
func (p *localProcess) Host() string          { return p.host }
func (p *localProcess) Port() int             { return p.port }
func (p *localProcess) Endpoints() []Endpoint { return []Endpoint{{"default", p.host, p.port}} }
func (p *localProcess) Done() <-chan struct{} { return p.done }
func (p *localProcess) Err() error            { p.errMu.RLock(); defer p.errMu.RUnlock(); return p.err }
func (p *localProcess) Stop(ctx context.Context) error {
	select {
	case <-p.done:
		return p.groupError()
	default:
	}
	p.stopOnce.Do(func() { _ = terminateProcessGroup(p.command) })
	select {
	case <-p.done:
		return p.groupError()
	case <-ctx.Done():
		killErr := killProcessGroup(p.command)
		return errors.Join(ctx.Err(), killErr)
	}
}

// AvailablePort chooses an unused local TCP port. Binding remains subject to the
// normal close-to-launch race; applications needing reservation should inherit
// their own pre-bound listeners through a custom driver.
func AvailablePort(host string) (int, error) { return availablePort(host) }
func availablePort(host string) (int, error) {
	l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
func waitForListener(ctx context.Context, p Process) error {
	for _, e := range p.Endpoints() {
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			select {
			case <-p.Done():
				return fmt.Errorf("process exited before readiness: %v", p.Err())
			default:
			}
			conn, err := (&net.Dialer{Timeout: 50 * time.Millisecond}).DialContext(ctx, "tcp", net.JoinHostPort(e.Host, strconv.Itoa(e.Port)))
			if err == nil {
				_ = conn.Close()
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-p.Done():
				return fmt.Errorf("process exited before readiness: %v", p.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	return ctx.Err()
}

func privateDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return fmt.Errorf("local state directory must be private (0700): %s", path)
	}
	return nil
}
func localSocketPath(dir, key string) (string, error) {
	runtimeDir := filepath.Join(os.TempDir(), "workload-"+strconv.Itoa(os.Getuid()))
	if err := privateDirectory(runtimeDir); err != nil {
		return "", err
	}
	path := filepath.Join(runtimeDir, identityHash(dir + "\x00" + key)[:32]+".sock")
	if len(path) > 100 {
		return "", fmt.Errorf("local supervisor socket path is too long; use a shorter TMPDIR")
	}
	return path, nil
}

var _ Driver = (*LocalDriver)(nil)
var _ Process = (*supervisedProcess)(nil)

func (p *localProcess) groupError() error {
	p.errMu.RLock()
	defer p.errMu.RUnlock()
	return p.groupErr
}
