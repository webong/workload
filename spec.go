package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	DesiredRunning   = "running"
	DesiredStopped   = "stopped"
	DriverLocal      = "local"
	DriverDocker     = "docker"
	DriverKubernetes = "kubernetes"
)

// Spec is one assigned instance. The caller expands replicas into assignments.
// LeaseUntil is required by Manager; it is never implicitly renewed.
type Spec struct {
	LeaseUntil    time.Time       `json:"lease_until,omitempty"`
	AssignmentID  string          `json:"assignment_id"`
	TargetID      string          `json:"target_id,omitempty"`
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Driver        string          `json:"driver"`
	DesiredState  string          `json:"desired_state"`
	Revision      int64           `json:"revision"`
	Configuration json.RawMessage `json:"configuration"`
}

func (s Spec) Validate() error {
	if len(s.Configuration) > 0 && !json.Valid(s.Configuration) {
		return fmt.Errorf("invalid workload configuration JSON")
	}
	if strings.TrimSpace(s.AssignmentID) == "" {
		return fmt.Errorf("provisioned server assignment ID is required")
	}
	if strings.TrimSpace(s.ID) == "" {
		return fmt.Errorf("provisioned server ID is required")
	}
	if strings.TrimSpace(s.Type) == "" {
		return fmt.Errorf("provisioned server %q type is required", s.ID)
	}
	if strings.TrimSpace(s.Driver) == "" {
		return fmt.Errorf("provisioned server %q uses unsupported driver %q", s.ID, s.Driver)
	}
	if s.DesiredState != DesiredRunning && s.DesiredState != DesiredStopped {
		return fmt.Errorf("provisioned server %q has invalid desired state %q", s.ID, s.DesiredState)
	}
	if s.Revision < 1 {
		return fmt.Errorf("provisioned server %q revision must be positive", s.ID)
	}

	return nil
}

// LaunchSpec describes one long-running process. No endpoints means readiness
// after process creation; it does not imply durable completion of a one-shot job.
type LaunchSpec struct {
	Endpoints        []EndpointSpec
	Limits           ResourceLimits
	Image            string
	Executable       string
	Arguments        []string
	Environment      []string
	WorkingDirectory string
	Port             int
	ContainerName    string
}

// Definition describes a managed technology independently of the host that runs it.
// Validate checks technology configuration; Prepare returns a specification
// without starting processes. The host selects the runtime and executes it.
type Definition interface {
	Type() string
	Supports(runtime string) bool
	Validate(context.Context, Spec) error
	Prepare(context.Context, Spec, Placement) (LaunchSpec, error)
}

// EndpointSpec names a TCP listener. Local definitions choose free ports using
// AvailablePort when they need dynamic allocation. Containers use fixed ports.
type EndpointSpec struct {
	Name string
	Port int
}

// Endpoint is an observed, ready TCP listener.
type Endpoint struct {
	Name string
	Host string
	Port int
}

// ResourceLimits are hard container ceilings. Zero leaves a ceiling unspecified.
// Local execution rejects limits rather than silently ignoring them.
type ResourceLimits struct {
	MemoryBytes int64
	MilliCPU    int64
	PIDs        int64
}

func (l LaunchSpec) endpoints() []EndpointSpec {
	if len(l.Endpoints) != 0 {
		return append([]EndpointSpec(nil), l.Endpoints...)
	}
	if l.Port != 0 {
		return []EndpointSpec{{Name: "default", Port: l.Port}}
	}
	return nil
}
func (l LaunchSpec) validate(container bool) error {
	if strings.TrimSpace(l.Executable) == "" {
		return fmt.Errorf("workload executable is required")
	}
	if container && strings.TrimSpace(l.Image) == "" {
		return fmt.Errorf("container image is required")
	}
	if l.Limits.MemoryBytes < 0 || l.Limits.MilliCPU < 0 || l.Limits.PIDs < 0 || l.Limits.MilliCPU > 9223372036854 {
		return fmt.Errorf("resource limits are negative or exceed supported bounds")
	}
	if l.Port != 0 && len(l.Endpoints) > 0 {
		return fmt.Errorf("use named Endpoints or legacy Port, not both")
	}
	seen := map[string]bool{}
	ports := map[int]bool{}
	for _, e := range l.endpoints() {
		if e.Name == "" || seen[e.Name] || e.Port < 1 || e.Port > 65535 || ports[e.Port] {
			return fmt.Errorf("invalid or duplicate endpoint %q:%d", e.Name, e.Port)
		}
		seen[e.Name] = true
		ports[e.Port] = true
	}
	for _, v := range l.Environment {
		key, _, ok := strings.Cut(v, "=")
		if !ok || key == "" {
			return fmt.Errorf("invalid environment entry")
		}
	}
	return nil
}

// Placement tells a definition which runtime and bind address it is preparing
// for. Ports contains reusable named ports during local recovery. A definition
// allocating dynamic named ports must reuse existing entries before allocating
// new ones with AvailablePort; it must not bind listeners or launch processes.
type Placement struct {
	Driver   string
	BindHost string
	Ports    map[string]int
}
