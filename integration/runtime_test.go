//go:build linux || darwin

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/webong/workload"
)

type fixtureDefinition struct{ image string }

func (fixtureDefinition) Type() string { return "integration-http" }
func (fixtureDefinition) Supports(driver string) bool {
	return driver == workload.DriverDocker || driver == workload.DriverKubernetes
}
func (fixtureDefinition) Validate(ctx context.Context, s workload.Spec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var config struct{ Marker string }
	return json.Unmarshal(s.Configuration, &config)
}
func (d fixtureDefinition) Prepare(ctx context.Context, s workload.Spec, p workload.Placement) (workload.LaunchSpec, error) {
	if err := d.Validate(ctx, s); err != nil {
		return workload.LaunchSpec{}, err
	}
	var config struct{ Marker string }
	_ = json.Unmarshal(s.Configuration, &config)
	limits := workload.ResourceLimits{MemoryBytes: 128 << 20, MilliCPU: 500}
	if p.Driver == workload.DriverDocker {
		limits.PIDs = 128
	}
	return workload.LaunchSpec{Image: d.image, Executable: "/workload-fixture", Environment: []string{"WORKLOAD_FIXTURE_REVISION=" + strconv.FormatInt(s.Revision, 10), "WORKLOAD_FIXTURE_MARKER=" + config.Marker}, Endpoints: []workload.EndpointSpec{{Name: "http", Port: 8080}, {Name: "metrics", Port: 9090}}, Limits: limits}, nil
}
func uniqueID() string {
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(err)
	}
	return "wl-it-" + hex.EncodeToString(id[:])
}
func requiredEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Fatalf("%s is required when integration testing is enabled", key)
	}
	return v
}
func dockerConfig(t *testing.T) workload.DockerDriverConfig {
	t.Helper()
	return workload.DockerDriverConfig{NodeID: uniqueID(), TargetID: uniqueID(), Binary: "docker", Context: requiredEnv(t, "WORKLOAD_TEST_DOCKER_CONTEXT"), Network: requiredEnv(t, "WORKLOAD_TEST_DOCKER_NETWORK"), PublishHost: "127.0.0.1", StartTimeout: 90 * time.Second, StopTimeout: 30 * time.Second}
}
func assignment(driver string) workload.Spec {
	return workload.Spec{ID: uniqueID(), AssignmentID: uniqueID(), Type: "integration-http", Driver: driver, DesiredState: workload.DesiredRunning, Revision: 1, Configuration: json.RawMessage(`{"Marker":"original"}`), LeaseUntil: time.Now().Add(10 * time.Minute)}
}

type identity struct{ BootID, Hostname, Revision, Marker string }

func readIdentity(t *testing.T, p workload.Process) identity {
	t.Helper()
	return readEndpointIdentities(t, p.Endpoints())
}
func readEndpointIdentities(t *testing.T, endpoints []workload.Endpoint) identity {
	t.Helper()
	if len(endpoints) != 2 {
		t.Fatalf("expected two named endpoints, got %+v", endpoints)
	}
	seen := map[string]bool{}
	var first identity
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	defer client.CloseIdleConnections()
	for _, endpoint := range endpoints {
		if endpoint.Name != "http" && endpoint.Name != "metrics" {
			t.Fatalf("unexpected endpoint %q", endpoint.Name)
		}
		if seen[endpoint.Name] {
			t.Fatalf("duplicate endpoint %q", endpoint.Name)
		}
		seen[endpoint.Name] = true
		url := "http://" + net.JoinHostPort(endpoint.Host, strconv.Itoa(endpoint.Port)) + "/identity"
		deadline := time.Now().Add(5 * time.Second)
		var current identity
		var lastErr error
		for time.Now().Before(deadline) {
			response, err := client.Get(url)
			if err == nil {
				err = json.NewDecoder(response.Body).Decode(&current)
				_ = response.Body.Close()
				if response.StatusCode != http.StatusOK {
					err = fmt.Errorf("HTTP status %d", response.StatusCode)
				}
			}
			lastErr = err
			if err == nil && current.BootID != "" {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if lastErr != nil || current.BootID == "" {
			t.Fatalf("endpoint %s is not serving: %v", endpoint.Name, lastErr)
		}
		if first.BootID == "" {
			first = current
		} else if first != current {
			t.Fatalf("named endpoints reach different instances: %+v / %+v", first, current)
		}
	}
	return first
}
func cleanupProcess(t *testing.T, p workload.Process) {
	t.Helper()
	if p == nil {
		return
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := p.Stop(ctx); err != nil {
			t.Errorf("stop test workload: %v", err)
		}
	})
}
func assertStopped(t *testing.T, p workload.Process) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	if err := p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.Done():
	default:
		t.Fatal("Stop did not confirm termination")
	}
}

// Adapted from Gateway's real-container adoption smoke, using only workload's
// public driver seam and an independent HTTP boot ID to detect recreation.
func TestDockerIntegrationAdoptsRunningContainer(t *testing.T) {
	if os.Getenv("WORKLOAD_TEST_DOCKER") != "1" {
		t.Skip("run scripts/integration-test.sh docker")
	}
	definition := fixtureDefinition{requiredEnv(t, "WORKLOAD_TEST_IMAGE")}
	config := dockerConfig(t)
	spec := assignment(workload.DriverDocker)
	firstDriver, err := workload.NewDockerDriver(config)
	if err != nil {
		t.Fatal(err)
	}
	first, err := firstDriver.Start(t.Context(), spec, definition)
	cleanupProcess(t, first)
	if err != nil {
		t.Fatal(err)
	}
	before := readIdentity(t, first)
	if before.Revision != "1" || before.Marker != "original" {
		t.Fatalf("wrong launched configuration: %+v", before)
	}
	secondDriver, err := workload.NewDockerDriver(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = secondDriver.PrepareDesired(t.Context(), map[string]workload.Spec{spec.AssignmentID: spec}); err != nil {
		t.Fatal(err)
	}
	second, err := secondDriver.Start(t.Context(), spec, definition)
	cleanupProcess(t, second)
	if err != nil {
		t.Fatal(err)
	}
	if after := readIdentity(t, second); after != before {
		t.Fatalf("recovery recreated container: before=%+v after=%+v", before, after)
	}
	assertStopped(t, second)
}

// Runtime inspection uses the fixture's own hostname (container ID / Pod name),
// not private workload labels or resource-name hashing.
func dockerCLI(t *testing.T, c workload.DockerDriverConfig, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, c.Binary, append([]string{"--context", c.Context}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}
func assertDockerLimits(t *testing.T, c workload.DockerDriverConfig, hostname string) {
	t.Helper()
	out := dockerCLI(t, c, "inspect", "--format", "{{json .HostConfig}}", hostname)
	var limits struct{ Memory, NanoCpus, PidsLimit int64 }
	if err := json.Unmarshal([]byte(out), &limits); err != nil {
		t.Fatal(err)
	}
	if limits.Memory != 128<<20 || limits.NanoCpus != 500000000 || limits.PidsLimit != 128 {
		t.Fatalf("requested limits were not enforced: %+v", limits)
	}
}
func replacementLifecycle(t *testing.T, s workload.Spec, newDriver func() workload.Driver, definition fixtureDefinition, limits func(identity)) {
	t.Helper()
	driver := newDriver()
	current, err := driver.Start(t.Context(), s, definition)
	t.Cleanup(func() {
		if current != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := current.Stop(ctx); err != nil {
				t.Errorf("cleanup replacement: %v", err)
			}
		}
	})
	if err != nil {
		t.Fatalf("initial launch: %v", err)
	}
	before := readIdentity(t, current)
	limits(before)
	// Environment/configuration changes must replace even at the same revision.
	s.Configuration = json.RawMessage(`{"Marker":"changed"}`)
	old := current
	current, err = newDriver().Start(t.Context(), s, definition)
	if err != nil {
		t.Fatalf("replace configuration: %v", err)
	}
	changed := readIdentity(t, current)
	if changed.BootID == before.BootID || changed.Marker != "changed" || changed.Revision != "1" {
		t.Fatalf("configuration change was not applied: before=%+v after=%+v", before, changed)
	}
	// A stale handle must never stop the replacement Pod. Docker uses immutable
	// container IDs, so its already-removed old handle is an idempotent no-op.
	stopErr := old.Stop(t.Context())
	if s.Driver == workload.DriverKubernetes && stopErr == nil {
		t.Fatal("stale Pod handle accepted the replacement identity")
	}
	if s.Driver == workload.DriverDocker && stopErr != nil {
		t.Fatal(stopErr)
	}
	if after := readIdentity(t, current); after != changed {
		t.Fatal("stale handle changed the replacement")
	}
	s.Revision = 2
	current, err = newDriver().Start(t.Context(), s, definition)
	if err != nil {
		t.Fatalf("replace revision: %v", err)
	}
	upgraded := readIdentity(t, current)
	if upgraded.BootID == changed.BootID || upgraded.Revision != "2" || upgraded.Marker != "changed" {
		t.Fatalf("revision replacement failed: before=%+v after=%+v", changed, upgraded)
	}
	limits(upgraded)
	assertStopped(t, current)
}
func TestDockerIntegrationReplacesConfigurationAndRevision(t *testing.T) {
	if os.Getenv("WORKLOAD_TEST_DOCKER") != "1" {
		t.Skip("run scripts/integration-test.sh docker")
	}
	c := dockerConfig(t)
	definition := fixtureDefinition{requiredEnv(t, "WORKLOAD_TEST_IMAGE")}
	replacementLifecycle(t, assignment(workload.DriverDocker), func() workload.Driver {
		d, err := workload.NewDockerDriver(c)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}, definition, func(id identity) { assertDockerLimits(t, c, id.Hostname) })
}

type recoveringDriver interface {
	workload.Driver
	PrepareDesired(context.Context, map[string]workload.Spec) error
}

func cleanupIsolation(t *testing.T, kind string, owned, foreign recoveringDriver, definition fixtureDefinition) {
	t.Helper()
	keepSpec := assignment(kind)
	staleSpec := assignment(kind)
	foreignSpec := assignment(kind)
	keep, err := owned.Start(t.Context(), keepSpec, definition)
	cleanupProcess(t, keep)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := owned.Start(t.Context(), staleSpec, definition)
	cleanupProcess(t, stale)
	if err != nil {
		t.Fatal(err)
	}
	other, err := foreign.Start(t.Context(), foreignSpec, definition)
	cleanupProcess(t, other)
	if err != nil {
		t.Fatal(err)
	}
	keepBefore := readIdentity(t, keep)
	foreignBefore := readIdentity(t, other)
	if err = owned.PrepareDesired(t.Context(), map[string]workload.Spec{keepSpec.AssignmentID: keepSpec}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stale.Done():
	case <-time.After(60 * time.Second):
		t.Fatal("stale owned resource was not removed")
	}
	if id := readIdentity(t, keep); id != keepBefore {
		t.Fatal("desired instance was changed by cleanup")
	}
	if id := readIdentity(t, other); id != foreignBefore {
		t.Fatal("foreign instance was changed by cleanup")
	}
	if err = owned.PrepareDesired(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if id := readIdentity(t, other); id != foreignBefore {
		t.Fatal("empty desired cleanup removed a foreign instance")
	}
	assertStopped(t, other)
}
func TestDockerIntegrationCleanupPreservesForeignResources(t *testing.T) {
	if os.Getenv("WORKLOAD_TEST_DOCKER") != "1" {
		t.Skip("run scripts/integration-test.sh docker")
	}
	c := dockerConfig(t)
	other := c
	other.NodeID = uniqueID()
	own, err := workload.NewDockerDriver(c)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := workload.NewDockerDriver(other)
	if err != nil {
		t.Fatal(err)
	}
	cleanupIsolation(t, workload.DriverDocker, own, foreign, fixtureDefinition{requiredEnv(t, "WORKLOAD_TEST_IMAGE")})
}
