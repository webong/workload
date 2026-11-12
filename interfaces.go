package workload

import "context"

// ControlPlane supplies a complete assignment snapshot and receives observations.
// An error must mean unavailable, never an empty desired snapshot. Implementations
// must enforce exclusive worker ownership before returning assignments. Report may be called concurrently during shutdown. Methods
// must honor cancellation; the package does not provide durable fencing storage.
type ControlPlane interface {
	Assignments(context.Context, string, []string) ([]Spec, string, error)
	Report(context.Context, string, string, InstanceReport) error
}

// Endpoints receives ready instance endpoints and withdrawals. Calls are serial
// (including shutdown callbacks) and must return promptly. Routing belongs to callers.
type Endpoints interface {
	Add(Spec, string, []Endpoint)
	Remove(string, string)
}

// DiscardEndpoints is useful for workers that do not publish routes.
type DiscardEndpoints struct{}

func (DiscardEndpoints) Add(Spec, string, []Endpoint) {}
func (DiscardEndpoints) Remove(string, string)        {}
