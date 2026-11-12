package workload

import (
	"context"
	"fmt"
	"sort"
)

type Starter interface {
	Start(context.Context, Spec) (Process, error)
}

type DriverRegistry struct {
	drivers   map[string]Driver
	workloads map[string]Definition
}

type driverPreparer interface {
	PrepareDesired(context.Context, map[string]Spec) error
}

func NewDriverRegistry(drivers map[string]Driver, workloads []Definition) (*DriverRegistry, error) {
	configured := make(map[string]Driver, len(drivers))
	for name, driver := range drivers {
		if driver == nil {
			return nil, fmt.Errorf("provisioning %s driver is nil", name)
		}
		configured[name] = driver
	}
	if len(configured) == 0 {
		return nil, fmt.Errorf("at least one provisioning runtime driver is required")
	}
	configuredWorkloads := make(map[string]Definition, len(workloads))
	for _, workload := range workloads {
		if workload == nil || workload.Type() == "" {
			return nil, fmt.Errorf("provisioning workload type is required")
		}
		if _, exists := configuredWorkloads[workload.Type()]; exists {
			return nil, fmt.Errorf("provisioning workload %q is registered twice", workload.Type())
		}
		configuredWorkloads[workload.Type()] = workload
	}
	if len(configuredWorkloads) == 0 {
		return nil, fmt.Errorf("at least one provisioning workload is required")
	}

	return &DriverRegistry{drivers: configured, workloads: configuredWorkloads}, nil
}

func (r *DriverRegistry) Start(ctx context.Context, spec Spec) (Process, error) {
	if err := r.Validate(ctx, spec); err != nil {
		return nil, err
	}
	return r.drivers[spec.Driver].Start(ctx, spec, r.workloads[spec.Type])
}

// PrepareDesired removes stale owned resources only after the control plane has
// supplied a successful assignment snapshot. Desired resources remain in place
// for the driver's Start method to adopt.
func (r *DriverRegistry) PrepareDesired(ctx context.Context, specifications []Spec) error {
	desired := make(map[string]map[string]Spec, len(r.drivers))
	for _, spec := range specifications {
		if err := r.Validate(ctx, spec); err != nil {
			return err
		}
		if _, exists := r.drivers[spec.Driver]; !exists {
			return fmt.Errorf("provisioning %s driver is not configured on this worker", spec.Driver)
		}
		if desired[spec.Driver] == nil {
			desired[spec.Driver] = make(map[string]Spec)
		}
		desired[spec.Driver][spec.AssignmentID] = spec
	}
	names := make([]string, 0, len(r.drivers))
	for name := range r.drivers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		preparer, ok := r.drivers[name].(driverPreparer)
		if !ok {
			continue
		}
		if err := preparer.PrepareDesired(ctx, desired[name]); err != nil {
			return fmt.Errorf("prepare provisioning %s driver: %w", name, err)
		}
	}

	return nil
}

var _ Starter = (*DriverRegistry)(nil)

// Validate checks assignment and capability configuration without runtime effects.
func (r *DriverRegistry) Validate(ctx context.Context, s Spec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.Validate(); err != nil {
		return err
	}
	if _, ok := r.drivers[s.Driver]; !ok {
		return fmt.Errorf("driver %q is not configured", s.Driver)
	}
	definition, ok := r.workloads[s.Type]
	if !ok {
		return fmt.Errorf("definition %q is not configured", s.Type)
	}
	if !definition.Supports(s.Driver) {
		return fmt.Errorf("definition %q does not support %q", s.Type, s.Driver)
	}
	return definition.Validate(ctx, s)
}
