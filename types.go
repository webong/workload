package workload

import "context"

// InstanceReport is the worker’s observed state for one reconciled instance. It
// is sent to the control plane and is not part of the extension-authoring interface.
type InstanceReport struct {
	AssignmentID string     `json:"assignment_id"`
	WorkloadID   string     `json:"workload_id"`
	Endpoints    []Endpoint `json:"endpoints,omitempty"`
	NodeID       string     `json:"node_id"`
	Runtime      string     `json:"runtime"`
	PID          int        `json:"pid,omitempty"`
	State        string     `json:"state"`
	Revision     int64      `json:"revision"`
	Error        string     `json:"error,omitempty"`
}

// Driver executes a definition. It must honor context deadlines and may return
// a Process together with an error when creation succeeded but readiness or
// cleanup is unconfirmed. A nil Process with an error promises no owned resource
// was left behind.
type Driver interface {
	Start(context.Context, Spec, Definition) (Process, error)
}

// Process is a running definition-owned workload managed by the worker.
// A failed Stop must leave Done open until termination is confirmed. Observing
// an API or transport failure is not proof of process exit. Stop is retryable.
type Process interface {
	Endpoints() []Endpoint
	PID() int
	Done() <-chan struct{}
	Err() error
	Stop(context.Context) error
}
