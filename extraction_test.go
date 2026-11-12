package workload

import (
	"context"
	"errors"
	"testing"
	"time"
)

func extractionReconciler(t *testing.T) (*Reconciler, *fakeControlPlane, *fakeDriver) {
	t.Helper()
	control := &fakeControlPlane{servers: []Spec{validSpec()}}
	driver := &fakeDriver{}
	r, err := NewReconciler(ReconcilerConfig{NodeID: "node", Drivers: []string{DriverLocal}, PollInterval: time.Second, StopTimeout: time.Second, CoordinationLease: time.Second}, control, driver, DiscardEndpoints{}, &fakeLogger{})
	if err != nil {
		t.Fatal(err)
	}
	return r, control, driver
}

func TestStoppedAssignmentDoesNotLaunch(t *testing.T) {
	r, c, d := extractionReconciler(t)
	r.reconcile(t.Context())
	c.servers[0].DesiredState = DesiredStopped
	r.reconcile(t.Context())
	r.reconcile(t.Context())
	if d.starts != 1 || d.stops != 1 || len(r.processes) != 0 {
		t.Fatalf("starts=%d stops=%d live=%d", d.starts, d.stops, len(r.processes))
	}
}

func TestInvalidSnapshotPreservesLiveInstancesUntilLeaseExpiry(t *testing.T) {
	r, c, d := extractionReconciler(t)
	r.reconcile(t.Context())
	c.servers = []Spec{{}}
	r.reconcile(t.Context())
	if d.stops != 0 {
		t.Fatal("invalid snapshot removed running instance")
	}
	r.lastAssignment = time.Now().Add(-2 * time.Second)
	r.reconcile(t.Context())
	if d.stops != 1 {
		t.Fatal("invalid snapshots renewed ownership")
	}
}

func TestDuplicateSnapshotDoesNotLaunchOrCleanup(t *testing.T) {
	r, c, d := extractionReconciler(t)
	c.servers = append(c.servers, c.servers[0])
	r.reconcile(t.Context())
	if d.starts != 0 {
		t.Fatal("accepted duplicate assignments")
	}
}

type unavailableControl struct{}

func (unavailableControl) Assignments(context.Context, string, []string) ([]Spec, string, error) {
	return nil, "", errors.New("offline")
}
func (unavailableControl) Report(context.Context, string, string, InstanceReport) error { return nil }

func TestUnavailableControlPlaneExpiresOwnership(t *testing.T) {
	r, _, d := extractionReconciler(t)
	r.reconcile(t.Context())
	r.control = unavailableControl{}
	r.reconcile(t.Context())
	if d.stops != 0 {
		t.Fatal("temporary outage stopped instance before lease expiry")
	}
	r.lastAssignment = time.Now().Add(-2 * time.Second)
	r.reconcile(t.Context())
	if d.stops != 1 {
		t.Fatal("expired owner retained instance")
	}
}

type refusesStop struct{ *fakeProcess }

func (*refusesStop) Stop(context.Context) error { return errors.New("runtime unavailable") }

func TestFailedStopDoesNotLaunchDuplicateReplacement(t *testing.T) {
	r, c, d := extractionReconciler(t)
	r.reconcile(t.Context())
	managed := r.processes[c.servers[0].AssignmentID]
	managed.process = &refusesStop{managed.process.(*fakeProcess)}
	c.servers[0].Revision++
	r.reconcile(t.Context())
	if d.starts != 1 || len(r.processes) != 1 {
		t.Fatal("failed stop lost ownership or launched replacement")
	}
}

func TestRunWithCancelledContextDoesNotLaunch(t *testing.T) {
	r, _, d := extractionReconciler(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r.Run(ctx)
	if d.starts != 0 {
		t.Fatal("cancelled worker launched a process")
	}
}
