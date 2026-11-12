package workload

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDriverRegistryDispatchesByServerDriverAndWorkload(t *testing.T) {
	local := &recordingRuntimeDriver{}
	docker := &recordingRuntimeDriver{}
	kubernetes := &recordingRuntimeDriver{}
	workload := &fakeWorkload{workloadType: "test"}
	registry, err := NewDriverRegistry(map[string]Driver{
		DriverLocal:      local,
		DriverDocker:     docker,
		DriverKubernetes: kubernetes,
	}, []Definition{workload})
	if err != nil {
		t.Fatal(err)
	}

	for driver, expected := range map[string]*recordingRuntimeDriver{
		DriverLocal: local, DriverDocker: docker, DriverKubernetes: kubernetes,
	} {
		spec := validSpec()
		spec.Driver = driver
		if _, err := registry.Start(t.Context(), spec); err != nil {
			t.Fatal(err)
		}
		if expected.starts != 1 || expected.workload != workload {
			t.Fatalf("expected %s driver and workload to receive start", driver)
		}
		expected.starts = 0
	}

	spec := validSpec()
	spec.Driver = DriverDocker
	localOnly, err := NewDriverRegistry(map[string]Driver{DriverLocal: local}, []Definition{workload})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := localOnly.Start(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("expected unconfigured driver error, got %v", err)
	}
}

func TestDriverRegistryPreparesOnlyDesiredAssignmentsPerRuntime(t *testing.T) {
	docker := &recordingRuntimeDriver{}
	kubernetes := &recordingRuntimeDriver{}
	registry, err := NewDriverRegistry(map[string]Driver{
		DriverDocker: docker, DriverKubernetes: kubernetes,
	}, []Definition{&fakeWorkload{workloadType: "test"}})
	if err != nil {
		t.Fatal(err)
	}
	dockerSpec := validSpec()
	dockerSpec.Driver = DriverDocker
	kubeSpec := validSpec()
	kubeSpec.Driver = DriverKubernetes
	kubeSpec.AssignmentID = "second-instance"
	if err := registry.PrepareDesired(t.Context(), []Spec{dockerSpec, kubeSpec}); err != nil {
		t.Fatal(err)
	}
	if len(docker.prepared) != 1 || docker.prepared[dockerSpec.AssignmentID].Driver != DriverDocker ||
		len(kubernetes.prepared) != 1 || kubernetes.prepared[kubeSpec.AssignmentID].Driver != DriverKubernetes {
		t.Fatalf("desired assignments crossed drivers: docker=%v kubernetes=%v", docker.prepared, kubernetes.prepared)
	}
}

func TestDockerRunArgumentsDescribeManagedWorkloadContainer(t *testing.T) {
	spec := validSpec()
	spec.Driver = DriverDocker
	config := DockerDriverConfig{
		TargetID: "target-a",
		NodeID:   "node-a", Network: "workload", EnvironmentFile: "", PublishHost: "127.0.0.1",
	}
	launch := LaunchSpec{
		Image: "registry.example.test/server:v1", Executable: "server",
		Arguments: []string{"serve", "--port=8080"}, Environment: []string{"SERVER_ID=server-1"}, Port: 8080,
	}
	args, err := dockerRunArguments(config, spec, "instance-1", "gateway-instance-1", launch)
	if err != nil {
		t.Fatal(err)
	}
	arguments := strings.Join(args, " ")
	for _, expected := range []string{
		"--name gateway-instance-1",
		"--network workload",

		"--publish 127.0.0.1::8080",
		"--env SERVER_ID=server-1",
		"--label workload.provisioning.type=" + runtimeLabelValue("test"),
		"--label workload.provisioning.node=" + runtimeLabelValue("node-a"),
		"--label workload.provisioning.target=" + runtimeLabelValue("target-a"),
		"--entrypoint server registry.example.test/server:v1 serve --port=8080",
	} {
		if !strings.Contains(arguments, expected) {
			t.Fatalf("expected Docker arguments to contain %q: %s", expected, arguments)
		}
	}
}

func TestDockerContainerIDIgnoresFirstPullProgress(t *testing.T) {
	output := "Unable to find image locally\nlayer: Pull complete\nsha256: digest\n0123456789abcdef"
	if id := dockerContainerID(output); id != "0123456789abcdef" {
		t.Fatalf("unexpected container ID %q", id)
	}
}

func TestKubernetesManifestDescribesManagedWorkloadPod(t *testing.T) {
	spec := validSpec()
	spec.Driver = DriverKubernetes
	config := KubernetesDriverConfig{
		TargetID: "target-a",
		NodeID:   "node-a", Namespace: "servers", ServiceAccount: "workload",
		EnvironmentSecret: "server-secrets", EnvironmentConfigMap: "server-config",
		ImagePullPolicy: "IfNotPresent", StopTimeout: 15 * time.Second,
	}
	launch := LaunchSpec{
		Image: "registry.example.test/server:v1", Executable: "server",
		Arguments: []string{"serve", "--port=8080"}, Environment: []string{"SERVER_ID=server-1"},
		Port: 8080, ContainerName: "server",
	}
	manifest, err := kubernetesPodManifest(config, spec, "instance-1", "gateway-instance-1", launch)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(manifest)
	for _, expected := range []string{
		`"kind":"Pod"`,
		`"name":"gateway-instance-1"`,
		`"image":"registry.example.test/server:v1"`,
		`"serviceAccountName":"workload"`,
		`"configMapRef":{"name":"server-config"}`,
		`"secretRef":{"name":"server-secrets"}`,
		`"name":"SERVER_ID","value":"server-1"`,
		`"workload.webong.dev/type":"` + runtimeLabelValue("test") + `"`,
		`"workload.webong.dev/node":"` + runtimeLabelValue("node-a") + `"`,
		`"workload.webong.dev/target":"` + runtimeLabelValue("target-a") + `"`,
		`"containerPort":8080`,
	} {
		if !strings.Contains(encoded, expected) {
			t.Fatalf("expected Kubernetes manifest to contain %q: %s", expected, encoded)
		}
	}
}

func TestSupportedDriverValidation(t *testing.T) {
	for _, driver := range []string{DriverLocal, DriverDocker, DriverKubernetes} {
		spec := validSpec()
		spec.Driver = driver
		if err := spec.Validate(); err != nil {
			t.Fatalf("expected %s to be supported: %v", driver, err)
		}
	}
	spec := validSpec()
	spec.Driver = ""
	if err := spec.Validate(); err == nil {
		t.Fatal("expected unsupported driver validation error")
	}
}

type recordingRuntimeDriver struct {
	starts   int
	workload Definition
	prepared map[string]Spec
}

func (d *recordingRuntimeDriver) PrepareDesired(_ context.Context, desired map[string]Spec) error {
	d.prepared = desired
	return nil
}

func (d *recordingRuntimeDriver) Start(_ context.Context, _ Spec, workload Definition) (Process, error) {
	d.starts++
	d.workload = workload
	return &fakeProcess{done: make(chan struct{})}, nil
}

type fakeWorkload struct {
	workloadType string
}

func (w *fakeWorkload) Type() string                       { return w.workloadType }
func (*fakeWorkload) Supports(string) bool                 { return true }
func (*fakeWorkload) Validate(context.Context, Spec) error { return nil }
func (*fakeWorkload) Prepare(context.Context, Spec, Placement) (LaunchSpec, error) {
	return LaunchSpec{}, nil
}

type recordingCommandRunner struct {
	outputs []string
	calls   []string
}

func (r *recordingCommandRunner) Output(_ context.Context, _ []byte, binary string, arguments ...string) (string, error) {
	r.calls = append(r.calls, strings.Join(append([]string{binary}, arguments...), " "))
	if len(r.outputs) == 0 {
		return "", nil
	}
	output := r.outputs[0]
	r.outputs = r.outputs[1:]
	return output, nil
}
