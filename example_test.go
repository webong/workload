package workload_test

import (
	"context"
	"fmt"
	"time"

	"github.com/webong/workload"
)

type serverDefinition struct{}

func (serverDefinition) Type() string                                  { return "example-server" }
func (serverDefinition) Supports(driver string) bool                   { return driver == workload.DriverDocker }
func (serverDefinition) Validate(context.Context, workload.Spec) error { return nil }
func (serverDefinition) Prepare(context.Context, workload.Spec, workload.Placement) (workload.LaunchSpec, error) {
	return workload.LaunchSpec{Image: "example/server@sha256:replace-with-image-digest", Executable: "server", Arguments: []string{"serve", "--port=8080"}, Endpoints: []workload.EndpointSpec{{Name: "http", Port: 8080}}, Limits: workload.ResourceLimits{MemoryBytes: 256 << 20, MilliCPU: 500}}, nil
}
func ExampleNewManager() {
	// This runnable example uses an in-memory driver. A real Docker driver also
	// performs discovery during shutdown, even when no assignments were started.
	driver := exampleDriver{}
	manager, err := workload.NewManager(workload.Options{NodeID: "worker-a", Drivers: map[string]workload.Driver{workload.DriverDocker: driver}, Definitions: []workload.Definition{serverDefinition{}}, PollInterval: time.Second, StartTimeout: 5 * time.Second, StopTimeout: 5 * time.Second})
	if err != nil {
		panic(err)
	}
	// The host obtains a complete snapshot from its authoritative control plane.
	// An empty snapshot starts no resources; this example never calls Docker.
	if err = manager.Apply(context.Background(), workload.Snapshot{}); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = manager.Run(ctx); err != nil {
		panic(err)
	}
	if err = manager.Shutdown(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println("worker stopped")
	// Output: worker stopped
}

// exampleDriver has no external runtime and no resources to recover. Its Start
// fails if an assignment is accidentally introduced into the empty example.
type exampleDriver struct{}

func (exampleDriver) Start(context.Context, workload.Spec, workload.Definition) (workload.Process, error) {
	return nil, fmt.Errorf("the runnable example has no execution backend")
}

// Real hosts can use this driver in the manager's Drivers map. This example is
// compiled without running against a Docker daemon.
func ExampleNewDockerDriver() {
	driver, err := workload.NewDockerDriver(workload.DockerDriverConfig{
		NodeID: "worker-a", Binary: "docker", PublishHost: "127.0.0.1",
		StartTimeout: 5 * time.Second, StopTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	_ = driver
}
