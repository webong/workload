package workload

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var ErrClosed = errors.New("workload manager is shut down")
var ErrNoSnapshot = errors.New("no assignment snapshot supplied")

type Snapshot struct{ Assignments []Spec }

// Options assembles a local manager. Drivers and definitions may not be mutated
// after construction. Driver calls must honor context cancellation.
type Options struct {
	NodeID                                  string
	Definitions                             []Definition
	Drivers                                 map[string]Driver
	PollInterval, StartTimeout, StopTimeout time.Duration
	Logger                                  Logger
}

// Manager accepts explicit leased snapshots independently of a control-plane
// transport. Apply is safe before Run and concurrently with Run/Status/Shutdown.
// One Run call owns reconciliation. Shutdown is terminal but retryable.
type Manager struct {
	wake            chan struct{}
	mu              sync.Mutex
	desired         Snapshot
	hasSnapshot     bool
	reports         map[string]InstanceReport
	registry        *DriverRegistry
	reconciler      *Reconciler
	running, closed bool
	cancel          context.CancelFunc
	done            chan struct{}
	shutdownGate    chan struct{}
}

func NewManager(o Options) (*Manager, error) {
	if o.PollInterval <= 0 || o.StartTimeout <= 0 || o.StopTimeout <= 0 {
		return nil, fmt.Errorf("manager intervals must be positive")
	}
	registry, err := NewDriverRegistry(o.Drivers, o.Definitions)
	if err != nil {
		return nil, err
	}
	if o.Logger == nil {
		o.Logger = discardLogger{}
	}
	m := &Manager{registry: registry, reports: map[string]InstanceReport{}, done: make(chan struct{}), wake: make(chan struct{}, 1), shutdownGate: make(chan struct{}, 1)}
	names := []string{}
	for k := range o.Drivers {
		names = append(names, k)
	}
	sort.Strings(names)
	r, err := NewReconciler(ReconcilerConfig{NodeID: o.NodeID, Drivers: names, PollInterval: o.PollInterval, StartTimeout: o.StartTimeout, StopTimeout: o.StopTimeout, CoordinationLease: o.StartTimeout}, managerControl{m}, registry, DiscardEndpoints{}, o.Logger)
	if err != nil {
		return nil, err
	}
	r.wake = m.wake
	m.reconciler = r
	return m, nil
}
func (m *Manager) Apply(ctx context.Context, s Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s = cloneSnapshot(s)
	seen := map[string]bool{}
	for _, a := range s.Assignments {
		if err := m.registry.Validate(ctx, a); err != nil {
			return err
		}
		if seen[a.AssignmentID] {
			return fmt.Errorf("duplicate assignment %q", a.AssignmentID)
		}
		seen[a.AssignmentID] = true
		if a.LeaseUntil.IsZero() || !time.Now().Before(a.LeaseUntil) {
			return fmt.Errorf("assignment %q requires a future lease deadline", a.AssignmentID)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.desired = s
	m.hasSnapshot = true
	select {
	case m.wake <- struct{}{}:
	default:
	}
	return nil
}
func (m *Manager) Status() []InstanceReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]InstanceReport, 0, len(m.reports))
	for _, r := range m.reports {
		r.Endpoints = append([]Endpoint(nil), r.Endpoints...)
		result = append(result, r)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].AssignmentID < result[j].AssignmentID })
	return result
}
func (m *Manager) Run(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	if m.running {
		m.mu.Unlock()
		return fmt.Errorf("manager already started")
	}
	m.running = true
	ctx, m.cancel = context.WithCancel(ctx)
	m.mu.Unlock()
	err := m.reconciler.Run(ctx)
	m.mu.Lock()
	m.closed = true
	close(m.done)
	m.mu.Unlock()
	return err
}
func (m *Manager) Shutdown(ctx context.Context) error {
	ctx, cancelBudget := context.WithTimeout(ctx, m.reconciler.config.StopTimeout)
	defer cancelBudget()
	// Cancel immediately; do not wait on a mutex held by a slow stop operation.
	m.mu.Lock()
	m.closed = true
	running := m.running
	cancel := m.cancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if running {
		select {
		case <-m.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case m.shutdownGate <- struct{}{}:
		defer func() { <-m.shutdownGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Retry any stop failures retained by Run, using the caller's total deadline.
	return m.reconciler.Shutdown(ctx)
}
func (m *Manager) assignments(ctx context.Context, _ string, _ []string) ([]Spec, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if !m.hasSnapshot {
		return nil, "", ErrNoSnapshot
	}
	return cloneSnapshot(m.desired).Assignments, "", nil
}
func (m *Manager) report(_ context.Context, _, _ string, r InstanceReport) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r.Endpoints = append([]Endpoint(nil), r.Endpoints...)
	if r.State != "running" {
		r.Endpoints = nil
	}
	m.reports[r.AssignmentID] = r
	return nil
}
func cloneSnapshot(s Snapshot) Snapshot {
	r := Snapshot{Assignments: append([]Spec(nil), s.Assignments...)}
	for i := range r.Assignments {
		r.Assignments[i].Configuration = append([]byte(nil), r.Assignments[i].Configuration...)
	}
	return r
}

type discardLogger struct{}

func (discardLogger) Debug(string, ...interface{}) {}
func (discardLogger) Info(string, ...interface{})  {}
func (discardLogger) Warn(string, ...interface{})  {}
func (discardLogger) Error(string, ...interface{}) {}

type managerControl struct{ m *Manager }

func (c managerControl) Assignments(ctx context.Context, node string, drivers []string) ([]Spec, string, error) {
	return c.m.assignments(ctx, node, drivers)
}
func (c managerControl) Report(ctx context.Context, workload, assignment string, r InstanceReport) error {
	return c.m.report(ctx, workload, assignment, r)
}
