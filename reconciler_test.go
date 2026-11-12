package workload

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestReconcilerConvergesReplicasAndReplacesRevisions(t *testing.T) {
	first := validSpec()
	second := validSpec()
	second.AssignmentID = "019d4000-0000-7000-8000-000000000002"
	control := &fakeControlPlane{servers: []Spec{first, second}}
	driver := &fakeDriver{}
	router := testEndpoints{}
	reconciler, err := NewReconciler(
		ReconcilerConfig{NodeID: "node-a", PollInterval: time.Second, StopTimeout: time.Second, CoordinationLease: 5 * time.Second},
		control,
		driver,
		router,
		&fakeLogger{},
	)
	if err != nil {
		t.Fatal(err)
	}

	reconciler.reconcile(t.Context())
	if len(reconciler.processes) != 2 || driver.starts != 2 {
		t.Fatalf("expected two replicas, processes=%d starts=%d", len(reconciler.processes), driver.starts)
	}

	control.servers[0].Revision = 2
	control.servers[1].Revision = 2
	reconciler.reconcile(t.Context())
	if len(reconciler.processes) != 2 || driver.starts != 4 || driver.stops != 2 {
		t.Fatalf("expected rolling replacement, processes=%d starts=%d stops=%d", len(reconciler.processes), driver.starts, driver.stops)
	}
	for _, process := range reconciler.processes {
		if process.spec.Revision != 2 {
			t.Fatalf("old revision remained: %+v", process.spec)
		}
	}

	control.servers = nil
	reconciler.reconcile(t.Context())
	if len(reconciler.processes) != 0 || driver.stops != 4 {
		t.Fatalf("expected all replicas stopped, processes=%d stops=%d", len(reconciler.processes), driver.stops)
	}
	if !control.reportedState("stopped") {
		t.Fatal("expected stopped instance report")
	}
}

func TestReconcilerReportsSelectedRuntime(t *testing.T) {
	spec := validSpec()
	spec.Driver = DriverDocker
	control := &fakeControlPlane{servers: []Spec{spec}}
	reconciler, err := NewReconciler(
		ReconcilerConfig{NodeID: "node-a", PollInterval: time.Second, StopTimeout: time.Second, CoordinationLease: 5 * time.Second},
		control,
		&fakeDriver{},
		testEndpoints{},
		&fakeLogger{},
	)
	if err != nil {
		t.Fatal(err)
	}

	reconciler.reconcile(t.Context())
	if len(control.reports) == 0 || control.reports[len(control.reports)-1].Runtime != DriverDocker {
		t.Fatalf("expected Docker runtime report, got %+v", control.reports)
	}
}

func TestReconcilerReportsProvisioningFailure(t *testing.T) {
	spec := validSpec()
	spec.Driver = DriverKubernetes
	control := &fakeControlPlane{servers: []Spec{spec}}
	reconciler, err := NewReconciler(
		ReconcilerConfig{NodeID: "node-a", PollInterval: time.Second, StopTimeout: time.Second, CoordinationLease: 5 * time.Second},
		control,
		&failingDriver{err: errors.New("pod admission denied")},
		testEndpoints{},
		&fakeLogger{},
	)
	if err != nil {
		t.Fatal(err)
	}

	reconciler.reconcile(t.Context())
	if len(control.reports) != 2 || control.reports[1].State != "failed" || control.reports[1].Runtime != DriverKubernetes || control.reports[1].Error != "pod admission denied" {
		t.Fatalf("unexpected provisioning failure report: %+v", control.reports)
	}
}

func TestReconcilerPreparesAfterSuccessfulAssignmentsBeforeStarting(t *testing.T) {
	spec := validSpec()
	control := &fakeControlPlane{servers: []Spec{spec}}
	driver := &preparingStarter{fakeDriver: &fakeDriver{}}
	reconciler, err := NewReconciler(
		ReconcilerConfig{NodeID: "node-a", PollInterval: time.Second, StopTimeout: time.Second, CoordinationLease: 5 * time.Second},
		control, driver, testEndpoints{}, &fakeLogger{},
	)
	if err != nil {
		t.Fatal(err)
	}
	reconciler.reconcile(t.Context())
	reconciler.reconcile(t.Context())
	if driver.prepares != 2 || driver.starts != 1 || !driver.preparedBeforeStart {
		t.Fatalf("expected preparation on each snapshot before adoption/start, prepares=%d starts=%d ordered=%v", driver.prepares, driver.starts, driver.preparedBeforeStart)
	}
}

type preparingStarter struct {
	*fakeDriver
	prepares            int
	preparedBeforeStart bool
}

func (d *preparingStarter) PrepareDesired(_ context.Context, specs []Spec) error {
	d.prepares++
	if len(specs) != 1 {
		return errors.New("missing desired assignment")
	}
	return nil
}

func (d *preparingStarter) Start(ctx context.Context, spec Spec) (Process, error) {
	d.preparedBeforeStart = d.prepares > 0
	return d.fakeDriver.Start(ctx, spec)
}

type fakeControlPlane struct {
	mu         sync.Mutex
	servers    []Spec
	reports    []InstanceReport
	resetNodes []string
}

func (f *fakeControlPlane) Assignments(context.Context, string, []string) ([]Spec, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Spec(nil), f.servers...), "node-a", nil
}

func (f *fakeControlPlane) Report(_ context.Context, _, _ string, report InstanceReport) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reports = append(f.reports, report)
	return nil
}

func (f *fakeControlPlane) ResetNode(_ context.Context, nodeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resetNodes = append(f.resetNodes, nodeID)
	return nil
}

func (f *fakeControlPlane) reportedState(state string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, report := range f.reports {
		if report.State == state {
			return true
		}
	}
	return false
}

type fakeDriver struct {
	starts int
	stops  int
}

type failingDriver struct {
	err error
}

func (f *failingDriver) Start(context.Context, Spec) (Process, error) {
	return nil, f.err
}

func (f *fakeDriver) Start(_ context.Context, _ Spec) (Process, error) {
	f.starts++
	return &fakeProcess{driver: f, pid: f.starts, port: 9000 + f.starts, done: make(chan struct{})}, nil
}

type fakeProcess struct {
	driver *fakeDriver
	pid    int
	port   int
	done   chan struct{}
	err    error
	once   sync.Once
}

func (f *fakeProcess) PID() int              { return f.pid }
func (f *fakeProcess) Host() string          { return "127.0.0.1" }
func (f *fakeProcess) Port() int             { return f.port }
func (f *fakeProcess) Done() <-chan struct{} { return f.done }
func (f *fakeProcess) Err() error            { return f.err }
func (f *fakeProcess) Stop(context.Context) error {
	f.once.Do(func() {
		f.driver.stops++
		close(f.done)
	})
	return nil
}

type fakeLogger struct{}

func (*fakeLogger) Debug(string, ...interface{}) {}
func (*fakeLogger) Info(string, ...interface{})  {}
func (*fakeLogger) Warn(string, ...interface{})  {}
func (*fakeLogger) Error(string, ...interface{}) {}

var _ ControlPlane = (*fakeControlPlane)(nil)
var _ Starter = (*fakeDriver)(nil)
var _ Process = (*fakeProcess)(nil)

func (f *fakeProcess) Endpoints() []Endpoint {
	return []Endpoint{{Name: "default", Host: f.Host(), Port: f.Port()}}
}
