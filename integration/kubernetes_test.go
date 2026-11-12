//go:build linux || darwin

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/webong/workload"
)

func kubernetesConfig(t *testing.T) workload.KubernetesDriverConfig {
	t.Helper()
	kubeconfig := requiredEnv(t, "WORKLOAD_TEST_KUBECONFIG")
	namespace := uniqueID()
	kubectl(t, kubeconfig, "create", "namespace", namespace)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		if t.Failed() {
			for _, args := range [][]string{{"get", "pods", "--namespace", namespace, "-o", "wide"}, {"describe", "pods", "--namespace", namespace}} {
				command := exec.CommandContext(ctx, "kubectl", append([]string{"--kubeconfig", kubeconfig, "--request-timeout=15s"}, args...)...)
				out, err := command.CombinedOutput()
				t.Logf("Kubernetes diagnostics: %s (%v)", out, err)
			}
		}
		out, err := exec.CommandContext(ctx, "kubectl", "--kubeconfig", kubeconfig, "--request-timeout=60s", "delete", "namespace", namespace, "--wait=true", "--timeout=60s").CombinedOutput()
		if err != nil {
			t.Errorf("delete integration namespace: %v: %s", err, out)
		}
	})
	return workload.KubernetesDriverConfig{NodeID: uniqueID(), TargetID: uniqueID(), Namespace: namespace, Kubeconfig: kubeconfig, KubectlBinary: "kubectl", ImagePullPolicy: "IfNotPresent", RouteMode: "port-forward", StartTimeout: 90 * time.Second, StopTimeout: 30 * time.Second}
}
func kubectl(t *testing.T, kubeconfig string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", append([]string{"--kubeconfig", kubeconfig, "--request-timeout=60s"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}
func newKubernetesDriver(t *testing.T, c workload.KubernetesDriverConfig) *workload.KubernetesDriver {
	t.Helper()
	d, err := workload.NewKubernetesDriver(c)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestKubernetesIntegrationAdoptsRunningPod(t *testing.T) {
	if os.Getenv("WORKLOAD_TEST_KUBERNETES") != "1" {
		t.Skip("run scripts/integration-test.sh kubernetes")
	}
	definition := fixtureDefinition{requiredEnv(t, "WORKLOAD_TEST_IMAGE")}
	config := kubernetesConfig(t)
	spec := assignment(workload.DriverKubernetes)
	driver := newKubernetesDriver(t, config)
	first, err := driver.Start(t.Context(), spec, definition)
	cleanupProcess(t, first)
	if err != nil {
		t.Fatal(err)
	}
	before := readIdentity(t, first)
	if before.Revision != "1" || before.Marker != "original" {
		t.Fatalf("wrong pod configuration: %+v", before)
	}
	recovery := newKubernetesDriver(t, config)
	if err = recovery.PrepareDesired(t.Context(), map[string]workload.Spec{spec.AssignmentID: spec}); err != nil {
		t.Fatal(err)
	}
	second, err := recovery.Start(t.Context(), spec, definition)
	cleanupProcess(t, second)
	if err != nil {
		t.Fatal(err)
	}
	if after := readIdentity(t, second); after != before {
		t.Fatalf("recovery recreated pod: before=%+v after=%+v", before, after)
	}
	assertStopped(t, second)
	// Public recovery cleanup must leave no managed Pod in this test's namespace.
	if remaining := kubectl(t, config.Kubeconfig, "get", "pods", "--namespace", config.Namespace, "-o", "name"); remaining != "" {
		t.Fatalf("Pod remained after Stop: %s", remaining)
	}
}

func assertKubernetesLimits(t *testing.T, c workload.KubernetesDriverConfig, hostname string) {
	t.Helper()
	out := kubectl(t, c.Kubeconfig, "get", "pod", hostname, "--namespace", c.Namespace, "-o", "json")
	var pod struct {
		Spec struct {
			Containers []struct {
				Resources struct{ Limits map[string]string }
			}
		}
	}
	if err := json.Unmarshal([]byte(out), &pod); err != nil {
		t.Fatal(err)
	}
	if len(pod.Spec.Containers) != 1 {
		t.Fatalf("unexpected containers: %+v", pod.Spec.Containers)
	}
	limits := pod.Spec.Containers[0].Resources.Limits
	if (limits["memory"] != "134217728" && limits["memory"] != "128Mi") || limits["cpu"] != "500m" {
		t.Fatalf("requested Pod limits were not enforced: %+v", limits)
	}
}
func TestKubernetesIntegrationReplacesConfigurationAndRevision(t *testing.T) {
	if os.Getenv("WORKLOAD_TEST_KUBERNETES") != "1" {
		t.Skip("run scripts/integration-test.sh kubernetes")
	}
	c := kubernetesConfig(t)
	s := assignment(workload.DriverKubernetes)
	s.TargetID = c.TargetID
	replacementLifecycle(t, s, func() workload.Driver { return newKubernetesDriver(t, c) }, fixtureDefinition{requiredEnv(t, "WORKLOAD_TEST_IMAGE")}, func(id identity) { assertKubernetesLimits(t, c, id.Hostname) })
}
func TestKubernetesIntegrationCleanupPreservesForeignResources(t *testing.T) {
	if os.Getenv("WORKLOAD_TEST_KUBERNETES") != "1" {
		t.Skip("run scripts/integration-test.sh kubernetes")
	}
	c := kubernetesConfig(t)
	other := c
	other.NodeID = uniqueID()
	cleanupIsolation(t, workload.DriverKubernetes, newKubernetesDriver(t, c), newKubernetesDriver(t, other), fixtureDefinition{requiredEnv(t, "WORKLOAD_TEST_IMAGE")})
}
