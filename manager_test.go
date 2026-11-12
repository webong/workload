package workload

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type managerDriver struct {
	mu            sync.Mutex
	starts, stops int
	stopError     error
	stopDelay     time.Duration
	startDelay    time.Duration
	partial       bool
}

func (d *managerDriver) Start(ctx context.Context, s Spec, _ Definition) (Process, error) {
	d.mu.Lock()
	delay := d.startDelay
	partial := d.partial
	d.mu.Unlock()
	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	d.mu.Lock()
	d.starts++
	d.mu.Unlock()
	p := &managerProcess{driver: d, done: make(chan struct{})}
	if partial {
		return p, errors.New("readiness failed")
	}
	return p, nil
}

type managerProcess struct {
	driver *managerDriver
	done   chan struct{}
	once   sync.Once
}

func (p *managerProcess) PID() int     { return 1 }
func (p *managerProcess) Host() string { return "127.0.0.1" }
func (p *managerProcess) Port() int    { return 9000 }
func (p *managerProcess) Endpoints() []Endpoint {
	return []Endpoint{{"http", "127.0.0.1", 9000}, {"metrics", "127.0.0.1", 9001}}
}
func (p *managerProcess) Done() <-chan struct{} { return p.done }
func (p *managerProcess) Err() error            { return nil }
func (p *managerProcess) Stop(ctx context.Context) error {
	p.driver.mu.Lock()
	delay := p.driver.stopDelay
	err := p.driver.stopError
	p.driver.mu.Unlock()
	if delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	if err != nil {
		return err
	}
	p.once.Do(func() { p.driver.mu.Lock(); p.driver.stops++; p.driver.mu.Unlock(); close(p.done) })
	return nil
}
func newTestManager(t *testing.T, d *managerDriver, interval time.Duration) *Manager {
	t.Helper()
	m, err := NewManager(Options{NodeID: "node", Definitions: []Definition{&fakeWorkload{"test"}}, Drivers: map[string]Driver{DriverLocal: d}, PollInterval: interval, StartTimeout: time.Second, StopTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func leased(id string, duration time.Duration) Spec {
	s := validSpec()
	s.AssignmentID = id
	s.LeaseUntil = time.Now().Add(duration)
	return s
}
func waitUntil(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached")
}
func runManager(t *testing.T, m *Manager) chan error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- m.Run(t.Context()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := m.Shutdown(ctx); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	return result
}
func TestManagerApplyStatusAndLeaseExpiry(t *testing.T) {
	d := &managerDriver{}
	m := newTestManager(t, d, time.Hour)
	a := leased("one", 100*time.Millisecond)
	if err := m.Apply(t.Context(), Snapshot{[]Spec{a}}); err != nil {
		t.Fatal(err)
	}
	result := runManager(t, m)
	waitUntil(t, func() bool { s := m.Status(); return len(s) == 1 && s[0].State == "running" })
	status := m.Status()
	if len(status[0].Endpoints) != 2 {
		t.Fatal("named endpoints absent")
	}
	status[0].Endpoints[0].Port = 1
	if m.Status()[0].Endpoints[0].Port != 9000 {
		t.Fatal("Status leaked mutable state")
	}
	waitUntil(t, func() bool { return m.Status()[0].State == "stopped" })
	d.mu.Lock()
	count := d.starts
	d.mu.Unlock()
	if count != 1 {
		t.Fatal("expired assignment restarted")
	}
	if err := m.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
func TestManagerInvalidSnapshotIsAtomicAndApplyCopiesInput(t *testing.T) {
	d := &managerDriver{}
	m := newTestManager(t, d, time.Hour)
	a := leased("one", time.Second)
	snapshot := Snapshot{[]Spec{a}}
	if err := m.Apply(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Assignments[0].Configuration[0] = '!'
	snapshot.Assignments[0].ID = "mutated"
	invalid := leased("bad", time.Second)
	invalid.Driver = "unknown"
	if err := m.Apply(t.Context(), Snapshot{[]Spec{a, invalid}}); err == nil {
		t.Fatal("invalid snapshot accepted")
	}
	got, _, err := m.assignments(t.Context(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID == "mutated" || string(got[0].Configuration) != "{}" {
		t.Fatalf("desired state mutated: %+v", got)
	}
	if err := m.Apply(t.Context(), Snapshot{[]Spec{leased("duplicate", time.Second), leased("duplicate", time.Second)}}); err == nil {
		t.Fatal("duplicates accepted")
	}
	zero := leased("zero", time.Second)
	zero.LeaseUntil = time.Time{}
	if err := m.Apply(t.Context(), Snapshot{[]Spec{zero}}); err == nil {
		t.Fatal("unleased assignment accepted")
	}
}
func TestManagerLeaseRenewalAndImmediateApply(t *testing.T) {
	d := &managerDriver{}
	m := newTestManager(t, d, time.Hour)
	a := leased("one", 100*time.Millisecond)
	if err := m.Apply(t.Context(), Snapshot{[]Spec{a}}); err != nil {
		t.Fatal(err)
	}
	runManager(t, m)
	waitUntil(t, func() bool { s := m.Status(); return len(s) == 1 && s[0].State == "running" })
	a.LeaseUntil = time.Now().Add(time.Second)
	if err := m.Apply(t.Context(), Snapshot{[]Spec{a}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if m.Status()[0].State != "running" {
		t.Fatal("lease renewal did not wake reconciler")
	}
	d.mu.Lock()
	starts := d.starts
	d.mu.Unlock()
	if starts != 1 {
		t.Fatal("lease renewal restarted process")
	}
	if err := m.Apply(t.Context(), Snapshot{}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { return m.Status()[0].State == "stopped" })
}
func TestManagerShutdownReturnsFailuresAndCanRetry(t *testing.T) {
	failure := errors.New("cannot confirm termination")
	d := &managerDriver{stopError: failure}
	m := newTestManager(t, d, time.Millisecond)
	if err := m.Apply(t.Context(), Snapshot{[]Spec{leased("one", time.Second)}}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- m.Run(t.Context()) }()
	waitUntil(t, func() bool { s := m.Status(); return len(s) == 1 && s[0].State == "running" })
	if err := m.Shutdown(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("shutdown hid failure: %v", err)
	}
	if err := <-result; !errors.Is(err, failure) {
		t.Fatalf("Run hid failure: %v", err)
	}
	status := m.Status()
	if status[0].State != "stop-failed" || len(status[0].Endpoints) != 0 {
		t.Fatalf("invalid stop-failed status: %+v", status)
	}
	d.mu.Lock()
	d.stopError = nil
	d.mu.Unlock()
	if err := m.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := m.Apply(t.Context(), Snapshot{}); !errors.Is(err, ErrClosed) {
		t.Fatal("shutdown manager accepted Apply")
	}
}
func TestManagerShutdownUsesOneBudgetAcrossInstances(t *testing.T) {
	d := &managerDriver{stopDelay: time.Second}
	m := newTestManager(t, d, time.Millisecond)
	items := []Spec{}
	for _, id := range []string{"one", "two", "three", "four"} {
		items = append(items, leased(id, 5*time.Second))
	}
	if err := m.Apply(t.Context(), Snapshot{items}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- m.Run(t.Context()) }()
	waitUntil(t, func() bool {
		s := m.Status()
		if len(s) != 4 {
			return false
		}
		for _, r := range s {
			if r.State != "running" {
				return false
			}
		}
		return true
	})
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := m.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected caller deadline: %v", err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("shutdown exceeded total caller deadline")
	}
	if err := <-result; err == nil {
		t.Fatal("Run lost stop timeout")
	}
	d.mu.Lock()
	d.stopDelay = 0
	d.mu.Unlock()
	if err := m.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestManagerPartialStartIsRetainedUntilCleanup(t *testing.T) {
	d := &managerDriver{partial: true, stopError: errors.New("offline")}
	m := newTestManager(t, d, time.Millisecond)
	if err := m.Apply(t.Context(), Snapshot{[]Spec{leased("one", time.Second)}}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- m.Run(t.Context()) }()
	waitUntil(t, func() bool { s := m.Status(); return len(s) == 1 && s[0].State == "stop-failed" })
	d.mu.Lock()
	starts := d.starts
	d.mu.Unlock()
	if starts != 1 {
		t.Fatal("partial start launched duplicate")
	}
	if err := m.Shutdown(t.Context()); err == nil {
		t.Fatal("unresolved partial start disappeared")
	}
	<-result
	d.mu.Lock()
	d.stopError = nil
	d.mu.Unlock()
	if err := m.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestManagerConcurrentLifecycleAndSnapshots(t *testing.T) {
	d := &managerDriver{}
	m := newTestManager(t, d, time.Millisecond)
	runManager(t, m)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				_ = m.Apply(t.Context(), Snapshot{[]Spec{leased("one", time.Second)}})
				_ = m.Status()
			}
		}()
	}
	wg.Wait()
	if err := m.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type recoveryFails struct {
	*managerDriver
	failure error
}

func (d *recoveryFails) PrepareDesired(context.Context, map[string]Spec) error { return d.failure }
func TestRecoveryFailuresReachRunAndShutdown(t *testing.T) {
	failure := errors.New("cannot inspect owned resources")
	m, err := NewManager(Options{NodeID: "node", Definitions: []Definition{&fakeWorkload{"test"}}, Drivers: map[string]Driver{DriverLocal: &recoveryFails{&managerDriver{}, failure}}, PollInterval: time.Millisecond, StartTimeout: time.Second, StopTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Apply(t.Context(), Snapshot{}); err != nil {
		t.Fatal(err)
	}
	if err = m.Run(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("Run hid recovery failure: %v", err)
	}
	if err = m.Shutdown(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("Shutdown hid unresolved recovery: %v", err)
	}
}
