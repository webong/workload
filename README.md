# workload

[![CI](https://github.com/webong/workload/actions/workflows/ci.yml/badge.svg)](https://github.com/webong/workload/actions/workflows/ci.yml)

Standalone Go lifecycle management for long-running processes, with local,
Docker and Kubernetes drivers. No Gateway dependencies. Go 1.25+, standard library
only. Module path: `github.com/webong/workload`.

```sh
go get github.com/webong/workload@v0.1.0
```

## Host interface

```go
manager, err := workload.NewManager(workload.Options{
    NodeID:       "worker-a",
    Definitions: definitions,
    Drivers:     drivers,
    PollInterval: time.Second,
    StartTimeout: 10 * time.Second,
    StopTimeout:  5 * time.Second,
})

// Fetch a complete snapshot from your authoritative control plane.
err = manager.Apply(ctx, workload.Snapshot{Assignments: assignments})

// Run once in the host's lifecycle group.
err = manager.Run(ctx)

// Safe concurrently with Run; returns copied observations and named endpoints.
observations := manager.Status()

// Cancel, stop within a total budget, and confirm cleanup. Retry on failure.
err = manager.Shutdown(shutdownCtx)
```

[example_test.go](example_test.go) is a compilable assembly example. Product
routing, placement, durable ownership, secrets and control-plane transports
belong to the host. Consumers implement their own control plane or supply desired
state directly; a separate control-plane service is optional. See the
[runtime assignment and lease contract](docs/assignment-lease-contract.md) for
adapter responsibilities, clock handling and ownership guarantees.

`Apply` validates and copies the entire snapshot before accepting it. Each
assignment has its own ID, workload ID, kind, positive revision, driver,
configuration and future `LeaseUntil`. One assignment represents one instance;
the host expands replicas. An empty snapshot stops all assignments. An error
fetching desired state must not be converted into an empty snapshot.

Acceptance is asynchronous: `Apply` wakes reconciliation; `Status` reports
`starting`, `running`, `stopping`, `stopped`, `failed`, `unhealthy` or
`stop-failed`. Only `running` observations advertise ready endpoints. Renewing a
lease does not restart the process. Expired assignments cannot restart from an
old snapshot; the run loop wakes for lease expiry independently of polling.
Status retains the latest observation per assignment, including terminal states.

A definition implements:

```go
type Definition interface {
    Type() string
    Supports(driver string) bool
    Validate(context.Context, Spec) error
    Prepare(context.Context, Spec, Placement) (LaunchSpec, error)
}
```

Preparation is deterministic and has no process/listener side effects. Placement
contains the driver, bind host and reusable named local ports. Local definitions
must reuse those ports when recovering. `LaunchSpec.Endpoints` describes named
TCP listeners; all must accept connections before readiness. No endpoints means
readiness after creation, suitable for foreground background workers.
`LaunchSpec.Port` remains shorthand for one endpoint named `default`; it cannot
be combined with named endpoints. These are long-running workloads: an exited
process is restarted while its assignment is running, including exit status zero.
This is not a durable one-shot job queue.

`ResourceLimits` describes memory bytes, CPU millicores and PID count. Docker
enforces all three; Kubernetes enforces memory and CPU and rejects PID limits.
Local execution rejects resource limits rather than ignoring them. Custom driver
names are allowed; capabilities are validated against the configured registry.

## Recovery and ownership

Docker and Kubernetes use names scoped to exact node/target/assignment identities
and hashed label values. Case, punctuation and long identities do not collapse to
the same sanitized name. SHA-256-derived fingerprints cover assignment/workload
identity, revision, configuration, launch settings and driver configuration.
Matching resources are adopted. Owned stale resources are stopped and removed
before replacement; foreign resources are rejected. Docker also checks observed
command, explicit environment, directory and requested limits. Kubernetes checks
managed Pod fields and accepts equivalent canonical resource quantities.

Docker environment-file contents participate in the fingerprint. Treat files
and referenced Kubernetes secrets/config maps as immutable during a launch;
bump the revision when changing external configuration. Pin image digests when
image content must be stable across nodes and restarts.

Kubernetes deletion uses a server-side UID precondition and waits for disappearance
of that identity. API failures and deletion acceptance alone do not prove
termination. Docker operations use immutable container IDs after discovery.
Creation/readiness failures retain cleanup handles whenever the runtime outcome
is uncertain. Recovery failures reach `Run` and `Shutdown`, even without logging.

Local execution uses a separate companion supervisor on Linux and macOS:

```sh
go build -o ./bin/workload-supervisor ./cmd/workload-supervisor
```

Configure `LocalDriverConfig.NodeID`, an absolute `SupervisorBinary`, and a durable
`StateDirectory` owned by the worker user with permissions `0700`. The supervisor
owns a process group, holds an exclusive assignment lock, and exposes a private
Unix socket. A replacement worker reconnects to it, verifies the fingerprint and
reuses its ports. Workloads must stay in the foreground and keep descendants in
the process group. Shutdown sends TERM, escalates to KILL and reaps the child.
A durable terminal receipt allows shutdown confirmation after a lost RPC reply.

If the supervisor itself crashes, an unfinished journal blocks automatic relaunch.
The caller must verify termination before removing that journal. The library never
signals a numeric PID from a stale file. This deliberately preserves uncertainty
instead of risking duplicate execution or signaling a reused PID.

The host must establish exclusive durable worker ownership before running the
manager or invoking cleanup. Leases are enforced by the live manager; they do not
provide an external crash watchdog or cross-node fencing. Keep that authority in
your durable control plane. Labels intentionally differ from Gateway's, so this
module does not adopt or migrate existing Gateway resources.

## Shutdown contract

`Run` is single-use. Cancellation triggers shutdown and returns joined errors
identifying unresolved assignments. `Shutdown` is terminal, safe concurrently
with `Run`, and retryable after failure. It uses the earlier of its caller deadline
and configured `StopTimeout` across waiting, stopping and recovery cleanup, with
at most eight concurrent instance stops. Failed stops retain ownership and
withdraw readiness; they cannot trigger a duplicate replacement.

A custom driver's `Start` may return both a handle and an error when creation
succeeded but readiness/cleanup is uncertain. Returning no handle promises no
owned workload was left behind. `Process.Done` describes observed exit;
`Process.Stop` must also confirm resource cleanup, honor cancellation and allow
retries. Custom definitions, drivers and control-plane transports must honor
their contexts; Go cannot forcibly interrupt arbitrary blocking implementations.

The lower-level `Reconciler` accepts a `ControlPlane` and serialized named-endpoint
callbacks for hosts that prefer polling integration. It also supports the original
node-refresh lease ceiling. Prefer `Manager` for explicit leased snapshots and
concurrent lifecycle calls. `ControlPlane.Report` can run concurrently at shutdown.

## Validation

```sh
go test -race ./...
go vet ./...
```

GitHub Actions runs formatting, vet, builds and uncached race tests on Linux and
macOS, using both Go 1.25 and the latest stable Go release. The workflow runs on
pushes and pull requests and can also be started manually.

Tests cover local recovery after worker exit with real subprocesses and sockets,
process termination, revision/launch drift, atomic Pod UID deletion, lost creation
responses, named endpoints, resource limits, lease renewal/expiry, concurrent
snapshots, and bounded/retryable shutdown. Fast driver tests use stateful command
fakes; the separate integration jobs below exercise the real runtimes.
See [EXTRACTION.md](EXTRACTION.md) for provenance and compatibility changes.


### Real Docker and Kubernetes integration tests

With a running Docker daemon:

```sh
scripts/integration-test.sh docker
```

With Docker, Kind and kubectl installed:

```sh
scripts/integration-test.sh kubernetes
# Or run both suites sequentially:
scripts/integration-test.sh all
```

The script builds a small, dependency-free HTTP fixture image for the Docker
server's architecture. It selects the current Docker context unless
`WORKLOAD_TEST_DOCKER_CONTEXT` is set. Kubernetes uses a uniquely named disposable
Kind cluster and a private kubeconfig, never the current Kubernetes context.
Only this invocation's fixture containers, network, image and cluster are removed.

Both suites verify recovery without changing the fixture's random HTTP boot ID,
configuration/revision replacement, actual resource limits, stale-handle safety,
cleanup that preserves foreign-owned resources, and adoption after a hard-killed
worker. The recovered worker then shuts down gracefully. Both named endpoints
must reach the same process. Kubernetes uses real port-forward processes and the
API server; worker recovery creates fresh forwarders to the surviving Pod.

These cases were adapted from Gateway's Docker adoption/crash tests and isolated
Kind provisioning smoke, using only workload's public Manager and Driver seams.
No Planner, PHP application or Gateway checkout is needed to run them.

Ordinary `go test ./...` skips these tests unless their runtime-specific opt-in
flag (`WORKLOAD_TEST_DOCKER=1` or `WORKLOAD_TEST_KUBERNETES=1`) is set. Once enabled,
missing configuration or runtime failures fail the tests; they do not silently
skip. Use the script to supply the fixture image and isolated runtime settings.
GitHub Actions runs each real-runtime suite in its own Ubuntu job alongside the
Linux/macOS unit matrix. On failure, CI uploads scoped test/runtime logs for seven
days; private kubeconfig credentials are not uploaded.
