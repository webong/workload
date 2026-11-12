package workload

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Logger interface {
	Debug(string, ...interface{})
	Info(string, ...interface{})
	Warn(string, ...interface{})
	Error(string, ...interface{})
}

type ReconcilerConfig struct {
	NodeID            string
	Drivers           []string
	CoordinationLease time.Duration
	PollInterval      time.Duration
	StopTimeout       time.Duration
	StartTimeout      time.Duration
}

type managedProcess struct {
	instanceID string
	spec       Spec
	process    Process
	stopping   bool
}

type Reconciler struct {
	runStarted     atomic.Bool
	stateMu        sync.Mutex
	callbackMu     sync.Mutex
	wake           <-chan struct{}
	config         ReconcilerConfig
	control        ControlPlane
	driver         Starter
	router         Endpoints
	logger         Logger
	processes      map[string]*managedProcess
	failures       map[string]string
	lastAssignment time.Time
}

func NewReconciler(config ReconcilerConfig, control ControlPlane, driver Starter, router Endpoints, logger Logger) (*Reconciler, error) {
	if config.NodeID == "" {
		return nil, fmt.Errorf("provisioning reconciler node ID is required")
	}
	if config.PollInterval <= 0 || config.StopTimeout <= 0 || config.CoordinationLease <= 0 {
		return nil, fmt.Errorf("provisioning reconciler intervals must be positive")
	}
	if control == nil || driver == nil || router == nil || logger == nil {
		return nil, fmt.Errorf("provisioning reconciler dependencies are required")
	}

	if config.StartTimeout <= 0 {
		config.StartTimeout = config.CoordinationLease
	}
	return &Reconciler{
		config:    config,
		control:   control,
		driver:    driver,
		router:    router,
		logger:    logger,
		processes: make(map[string]*managedProcess),
		failures:  make(map[string]string),
	}, nil
}

// Run may be called once. On cancellation it returns a joined shutdown error;
// a non-nil error means the caller must retain fencing for unresolved instances.
func (r *Reconciler) Run(ctx context.Context) error {
	if !r.runStarted.CompareAndSwap(false, true) {
		return fmt.Errorf("reconciler already started")
	}
	if ctx.Err() != nil {
		return nil
	}
	for {
		if err := r.reconcile(ctx); err != nil {
			return errors.Join(err, r.Shutdown(context.Background()))
		}
		wait := r.config.PollInterval
		r.stateMu.Lock()
		for _, p := range r.processes {
			if p.stopping {
				if wait > 100*time.Millisecond {
					wait = 100 * time.Millisecond
				}
				continue
			}
			if !p.spec.LeaseUntil.IsZero() {
				if d := time.Until(p.spec.LeaseUntil); d < wait {
					wait = d
				}
			}
		}
		r.stateMu.Unlock()
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return r.Shutdown(context.Background())
		case <-r.wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// Shutdown uses one total timeout for all instances, attempts stops concurrently,
// returns every failure, and retains unresolved instances for a retry. It must
// not run concurrently with Run; Manager coordinates that handoff for callers.
func (r *Reconciler) Shutdown(ctx context.Context) error {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, r.config.StopTimeout)
	defer cancel()
	return r.shutdownContext(ctx)
}

func (r *Reconciler) reconcile(ctx context.Context) error {
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	if ctx.Err() != nil {
		return nil
	}
	fetchLimit := r.nextDeadline(r.config.CoordinationLease)

	fetchCtx, cancel := context.WithTimeout(ctx, fetchLimit)
	specifications, leaderID, err := r.control.Assignments(fetchCtx, r.config.NodeID, r.config.Drivers)
	cancel()
	if err != nil {
		r.logger.Error("Failed to load provisioned assignments: %v", err)
		r.expireIndividualLeases()
		r.expireAssignments()
		return nil
	}
	// Validate the entire snapshot before cleanup or changing live instances.
	seen := make(map[string]bool, len(specifications))
	for _, spec := range specifications {
		if err := spec.Validate(); err != nil {
			r.logger.Error("Invalid assignment snapshot: %v", err)
			r.expireIndividualLeases()
			r.expireAssignments()
			return nil
		}
		if seen[spec.AssignmentID] {
			r.logger.Error("Duplicate assignment %s", spec.AssignmentID)
			r.expireIndividualLeases()
			r.expireAssignments()
			return nil
		}
		seen[spec.AssignmentID] = true
	}
	// Stopped assignments have no runtime resource in the desired snapshot.
	running := make([]Spec, 0, len(specifications))
	for _, spec := range specifications {
		if spec.DesiredState == DesiredRunning && (spec.LeaseUntil.IsZero() || time.Now().Before(spec.LeaseUntil)) {
			running = append(running, spec)
		}
	}
	specifications = running
	for _, spec := range specifications {
		if current, ok := r.processes[spec.AssignmentID]; ok && sameExecution(spec, current.spec) {
			current.spec.LeaseUntil = spec.LeaseUntil
		}
	}
	r.expireIndividualLeases()
	r.lastAssignment = time.Now()
	if leaderID == r.config.NodeID {
		r.logger.Debug("Worker %s is the active provisioning placement leader", r.config.NodeID)
	}
	{
		if preparer, ok := r.driver.(interface {
			PrepareDesired(context.Context, []Spec) error
		}); ok {
			prepareCtx, prepareCancel := context.WithTimeout(ctx, r.nextDeadline(r.config.StartTimeout))
			prepareErr := preparer.PrepareDesired(prepareCtx, r.recoveryDesired(specifications))
			prepareCancel()
			if err := prepareErr; err != nil {
				r.logger.Error("Failed to prepare provisioned assignments: %v", err)
				return fmt.Errorf("recover workloads: %w", err)
			}
		}
	}

	specs := make(map[string]Spec, len(specifications))
	for _, spec := range specifications {
		specs[spec.AssignmentID] = spec
	}
	for assignmentID := range r.failures {
		if _, exists := specs[assignmentID]; !exists {
			delete(r.failures, assignmentID)
		}
	}

	ids := make([]string, 0, len(r.processes))
	for id := range r.processes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		managed := r.processes[id]
		select {
		case <-managed.process.Done():
			// Even after observed exit, remove the owned runtime resource. A failed
			// cleanup is retained and retried rather than forgetting the handle.
			if !r.stop(managed) {
				continue
			}
			r.withdraw(managed.spec.ID, managed.instanceID)
			r.reportWithinLease(ctx, managed, "failed", managed.process.Err())
			delete(r.processes, id)
			continue
		default:
		}

		if h, ok := managed.process.(interface{ Health() error }); ok {
			if err := h.Health(); err != nil {
				managed.stopping = true
				r.report(ctx, managed, "unhealthy", err)
			}
		}
		desired, exists := specs[managed.instanceID]
		if exists && sameExecution(desired, managed.spec) {
			managed.spec.LeaseUntil = desired.LeaseUntil
		}
		if managed.stopping || !exists || !sameExecution(desired, managed.spec) {
			if r.stop(managed) {
				delete(r.processes, id)
			}
		}
	}

	for _, spec := range specifications {
		if _, exists := r.processes[spec.AssignmentID]; exists {
			continue
		}
		if _, startErr := r.start(ctx, spec); startErr != nil {
			r.logger.Error("Failed to start provisioned server %s: %v", spec.ID, startErr)
		}
	}

	for _, managed := range r.processes {
		if !managed.stopping {
			r.reportWithinLease(ctx, managed, "running", nil)
		}
	}
	return nil
}

func (r *Reconciler) start(ctx context.Context, spec Spec) (*managedProcess, error) {
	instanceID := spec.AssignmentID
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if !spec.LeaseUntil.IsZero() && !time.Now().Before(spec.LeaseUntil) {
		return nil, fmt.Errorf("assignment lease expired")
	}
	startCtx, startCancel := context.WithTimeout(ctx, r.nextDeadline(r.config.StartTimeout))
	if !spec.LeaseUntil.IsZero() {
		var leaseCancel context.CancelFunc
		startCtx, leaseCancel = context.WithDeadline(startCtx, spec.LeaseUntil)
		defer leaseCancel()
	}
	defer startCancel()
	_ = r.control.Report(startCtx, spec.ID, instanceID, InstanceReport{AssignmentID: instanceID, WorkloadID: spec.ID, NodeID: r.config.NodeID, Runtime: spec.Driver, State: "starting", Revision: spec.Revision})
	process, err := r.driver.Start(startCtx, spec)
	// Start may return a handle together with an error when a resource was created
	// but readiness/cleanup could not be confirmed. Never lose that ownership.
	if process != nil && err != nil {
		managed := &managedProcess{instanceID: instanceID, spec: spec, process: process, stopping: true}
		r.processes[instanceID] = managed
		r.report(ctx, managed, "failed", err)
		return managed, err
	}
	if err != nil {
		r.failures[instanceID] = instanceID
		report := InstanceReport{
			AssignmentID: instanceID, WorkloadID: spec.ID,
			NodeID:   r.config.NodeID,
			Runtime:  spec.Driver,
			State:    "failed",
			Revision: spec.Revision,
			Error:    err.Error(),
		}
		reportCtx, cancel := context.WithTimeout(ctx, r.config.StopTimeout)
		defer cancel()
		if reportErr := r.control.Report(reportCtx, spec.ID, instanceID, report); reportErr != nil {
			r.logger.Warn("Failed to report provisioned instance %s: %v", instanceID, reportErr)
		}
		return nil, err
	}
	if process == nil {
		return nil, fmt.Errorf("driver returned nil process without error")
	}
	delete(r.failures, instanceID)
	managed := &managedProcess{instanceID: instanceID, spec: spec, process: process}
	r.processes[instanceID] = managed
	if ctx.Err() != nil || (!spec.LeaseUntil.IsZero() && !time.Now().Before(spec.LeaseUntil)) {
		managed.stopping = true
		if r.stop(managed) {
			delete(r.processes, instanceID)
		}
		return nil, fmt.Errorf("assignment cancelled or expired during startup")
	}
	if len(process.Endpoints()) > 0 {
		r.publish(spec, instanceID, process.Endpoints())
	}
	if !managed.stopping {
		r.reportWithinLease(ctx, managed, "running", nil)
	}
	r.logger.Info("Started workload instance %s for %s", instanceID, spec.ID)

	return managed, nil
}

func (r *Reconciler) stop(managed *managedProcess) bool {
	ctx, cancel := context.WithTimeout(context.Background(), r.nextDeadlineExcept(r.config.StopTimeout, managed.instanceID))
	defer cancel()
	return r.stopContext(ctx, managed) == nil
}
func (r *Reconciler) stopContext(ctx context.Context, managed *managedProcess) error {
	managed.stopping = true
	r.withdraw(managed.spec.ID, managed.instanceID)
	r.report(ctx, managed, "stopping", nil)
	err := managed.process.Stop(ctx)
	if err != nil {
		r.report(ctx, managed, "stop-failed", err)
		return err
	}
	select {
	case <-managed.process.Done():
	default:
		err = fmt.Errorf("driver Stop returned before confirming termination")
		r.report(ctx, managed, "stop-failed", err)
		return err
	}
	r.report(ctx, managed, "stopped", nil)
	return nil
}
func (r *Reconciler) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), r.config.StopTimeout)
	defer cancel()
	_ = r.shutdownContext(ctx)
}
func (r *Reconciler) shutdownContext(ctx context.Context) error {
	type result struct {
		id  string
		err error
	}
	results := make(chan result, len(r.processes))
	type job struct {
		id string
		p  *managedProcess
	}
	jobs := make(chan job, len(r.processes))
	for id, p := range r.processes {
		jobs <- job{id, p}
	}
	close(jobs)
	// Bound shutdown concurrency while sharing one deadline across the whole set.
	for range min(8, len(r.processes)) {
		go func() {
			for task := range jobs {
				results <- result{task.id, r.stopContext(ctx, task.p)}
			}
		}()
	}
	all := make([]result, 0, len(r.processes))
	for range len(r.processes) {
		all = append(all, <-results)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].id < all[j].id })
	var failures []error
	for _, result := range all {
		if result.err == nil {
			delete(r.processes, result.id)
		} else {
			failures = append(failures, fmt.Errorf("assignment %s: %w", result.id, result.err))
		}
	}

	if preparer, ok := r.driver.(interface {
		PrepareDesired(context.Context, []Spec) error
	}); ok {
		// Discover and clean up owned resources left by an interrupted creation or a
		// predecessor. Preserve unresolved handles for their normal retry path.
		if err := preparer.PrepareDesired(ctx, r.recoveryDesired(nil)); err != nil {
			failures = append(failures, fmt.Errorf("shutdown recovery: %w", err))
		}
	}
	return errors.Join(failures...)
}

func (r *Reconciler) report(ctx context.Context, managed *managedProcess, state string, processErr error) {
	ctx, cancel := context.WithTimeout(ctx, r.config.StopTimeout)
	defer cancel()
	report := InstanceReport{
		AssignmentID: managed.instanceID, WorkloadID: managed.spec.ID, Endpoints: managed.process.Endpoints(),
		NodeID:   r.config.NodeID,
		Runtime:  managed.spec.Driver,
		PID:      managed.process.PID(),
		State:    state,
		Revision: managed.spec.Revision,
	}
	if processErr != nil {
		report.Error = processErr.Error()
	}
	if err := r.control.Report(ctx, managed.spec.ID, managed.instanceID, report); err != nil {
		r.logger.Warn("Failed to report provisioned instance %s: %v", managed.instanceID, err)
	}
}

func (r *Reconciler) expireAssignments() {
	if !r.lastAssignment.IsZero() && time.Since(r.lastAssignment) >= r.config.CoordinationLease {
		r.logger.Warn("Assignment lease expired; stopping local workloads")
		r.shutdown()
	}
}

func sameExecution(a, b Spec) bool {
	a.LeaseUntil = time.Time{}
	b.LeaseUntil = time.Time{}
	return reflect.DeepEqual(a, b)
}
func (r *Reconciler) nextDeadline(limit time.Duration) time.Duration {
	for _, p := range r.processes {
		if !p.stopping && !p.spec.LeaseUntil.IsZero() {
			d := time.Until(p.spec.LeaseUntil)
			if d < limit {
				limit = d
			}
		}
	}
	if limit < time.Millisecond {
		return time.Millisecond
	}
	return limit
}
func (r *Reconciler) expireIndividualLeases() {
	for id, p := range r.processes {
		if p.stopping || (!p.spec.LeaseUntil.IsZero() && !time.Now().Before(p.spec.LeaseUntil)) {
			if r.stop(p) {
				delete(r.processes, id)
			}
		}
	}
}

func (r *Reconciler) withdraw(workloadID, assignmentID string) {
	r.callbackMu.Lock()
	defer r.callbackMu.Unlock()
	r.router.Remove(workloadID, assignmentID)
}
func (r *Reconciler) publish(s Spec, id string, endpoints []Endpoint) {
	r.callbackMu.Lock()
	defer r.callbackMu.Unlock()
	r.router.Add(s, id, append([]Endpoint(nil), endpoints...))
}

func (r *Reconciler) reportWithinLease(ctx context.Context, p *managedProcess, state string, err error) {
	ctx, cancel := context.WithTimeout(ctx, r.nextDeadline(r.config.StopTimeout))
	defer cancel()
	r.report(ctx, p, state, err)
}

func (r *Reconciler) recoveryDesired(desired []Spec) []Spec {
	result := append([]Spec(nil), desired...)
	seen := map[string]bool{}
	for _, s := range desired {
		seen[s.AssignmentID] = true
	}
	for id, p := range r.processes {
		if !seen[id] {
			result = append(result, p.spec)
		}
	}
	return result
}

func (r *Reconciler) nextDeadlineExcept(limit time.Duration, except string) time.Duration {
	for id, p := range r.processes {
		if id != except && !p.stopping && !p.spec.LeaseUntil.IsZero() {
			if d := time.Until(p.spec.LeaseUntil); d < limit {
				limit = d
			}
		}
	}
	if limit < time.Millisecond {
		return time.Millisecond
	}
	return limit
}
