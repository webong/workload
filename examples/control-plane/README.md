# A Go app with its own control plane

This runnable, single-node app implements `workload.ControlPlane` itself. It stores desired state in memory, assigns local replicas, renews their leases, and exposes observed state over HTTP. Workload runs the processes and reconciles their lifecycle. No Docker, Kubernetes, PHP, or external service is required.

Run these commands from the repository root on Linux or macOS with Go 1.25 or newer:

```sh
mkdir -p bin
go build -o bin/workload-supervisor ./cmd/workload-supervisor
go build -o bin/control-plane ./examples/control-plane
./bin/control-plane -supervisor ./bin/workload-supervisor
```

The control-plane API listens on `127.0.0.1:8080`. Supervisor state lives in `.workload-example/`. Use `-listen 127.0.0.1:9090` or `-state-dir /tmp/my-workload-demo` to change these defaults. One controller holds an exclusive lock on its state directory.

## Create and observe workloads

In another terminal, create two HTTP worker processes:

```sh
curl -fsS -X PUT http://127.0.0.1:8080/workloads/hello \
  -H 'Content-Type: application/json' \
  -d '{"message":"Hello from workload","replicas":2}'
curl -fsS http://127.0.0.1:8080/workloads
curl -fsS http://127.0.0.1:8080/instances
```

Writes return `202 Accepted`: reconciliation happens asynchronously. Poll `/instances` until both entries have `ready: true`. Each has an `endpoints` array containing a `url`; curl that URL to get the worker's message, revision, and actual process ID. Ports are assigned dynamically. Stable assignment IDs are `hello/0` and `hello/1`.

Change the message to replace both workers with revision 2:

```sh
curl -fsS -X PUT http://127.0.0.1:8080/workloads/hello \
  -H 'Content-Type: application/json' \
  -d '{"message":"Updated message","replicas":2}'
```

Scale down while keeping the same message:

```sh
curl -fsS -X PUT http://127.0.0.1:8080/workloads/hello \
  -H 'Content-Type: application/json' \
  -d '{"message":"Updated message","replicas":1}'
```

The surviving replica keeps its revision and PID. The removed replica loses its published endpoint immediately and is then stopped. There is no zero-downtime rollout policy in this example.

## Exercise lease expiry and shutdown

Pause assignment polling to simulate control-plane unavailability:

```sh
curl -fsS -X POST http://127.0.0.1:8080/control-plane/pause
curl -fsS http://127.0.0.1:8080/control-plane
```

The control plane returns polling errors rather than a successful empty assignment list. Existing workers continue until their last issued leases expire (ten seconds by default), then workload stops them. `/instances` withdraws endpoints when their leases expire. Set `-lease-duration 3s` at startup for a quicker demonstration.

Resume to issue fresh leases and restart the desired workers:

```sh
curl -fsS -X POST http://127.0.0.1:8080/control-plane/resume
curl -fsS http://127.0.0.1:8080/instances
```

Delete the desired workload to stop its remaining replica:

```sh
curl -fsS -X DELETE http://127.0.0.1:8080/workloads/hello
```

Press Ctrl-C in the controller terminal. The app closes HTTP admission, cancels the reconciler, and waits for supervised processes to stop before releasing ownership. An unresolved shutdown returns an error identifying the state directory to retain for recovery.

## Where the responsibilities live

| File | Application responsibility |
| --- | --- |
| `control_plane.go` | HTTP API, desired state, stable assignments, execution revisions, lease issuance, reports, and endpoint visibility |
| `worker.go` | Workload definition translating configuration into an executable and arguments; the HTTP worker itself |
| `main.go` | Wiring the local driver, definition registry, control plane, reconciler, and shutdown |
| `owner_unix.go` | Exclusive controller ownership of this host's state directory |

The key wiring is:

```go
plane := newControlPlane("demo-node", lease)
registry, err := workload.NewDriverRegistry(
    map[string]workload.Driver{workload.DriverLocal: driver},
    []workload.Definition{definition},
)
// Handle err, then:
reconciler, err := workload.NewReconciler(config, plane, registry,
    workload.DiscardEndpoints{}, logger)
// Handle err, then run until cancellation and handle its shutdown result:
err = reconciler.Run(ctx)
```

`Assignments` returns a complete snapshot for the node, with a future `LeaseUntil` on every assignment. `Report` receives lifecycle observations. This app exposes usable endpoints from those observations only while the assignment is still desired, its revision matches, and its lease is current. See [the assignment and lease contract](../../docs/assignment-lease-contract.md) for the runtime boundary.

This is deliberately an in-memory demonstration: desired state and observations reset when the controller restarts. Supervisor journals are durable, so the local driver can reconcile leftover processes against the next desired snapshot; they do not restore this app's desired state. Keep the state directory private and retain it if cleanup fails. The API is unauthenticated and restricted to loopback IP addresses. A production app supplies its own durable storage, authentication, placement, ownership/fencing, and rollout policies. The host lock shown here does not coordinate multiple machines.

## Tests

```sh
go test -race -count=1 ./examples/control-plane
```

HTTP contract tests cover assignments, revisions, and lease renewal. The end-to-end test builds both binaries and verifies actual worker responses, revision replacement, scaling without restarting the survivor, lease expiry, resumption, deletion, and graceful shutdown. It runs as part of the repository's regular Linux/macOS CI matrix.
