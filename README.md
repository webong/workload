# workload

[![CI](https://github.com/webong/workload/actions/workflows/ci.yml/badge.svg)](https://github.com/webong/workload/actions/workflows/ci.yml)

Run and manage long-running processes from your Go application, locally, in Docker,
or on Kubernetes. Workload starts them, waits for them to be ready, restarts them
if they exit, and stops them when your app no longer needs them.

Your app decides what should run and where. Workload handles running it.

Requires Go 1.25 or newer. The Go module uses only the standard library.

```sh
go get github.com/webong/workload@v0.2.1
```

## Try a complete app

The [Go example](examples/control-plane/) runs real HTTP worker processes and gives
you an API to create, update, scale, and stop them. It runs on Linux and macOS
without Docker or Kubernetes.

From a checkout of this repository:

```sh
mkdir -p bin
go build -o bin/workload-supervisor ./cmd/workload-supervisor
go build -o bin/control-plane ./examples/control-plane
./bin/control-plane -supervisor ./bin/workload-supervisor
```

In another terminal, start two workers:

```sh
curl -fsS -X PUT http://127.0.0.1:8080/workloads/hello \
  -H 'Content-Type: application/json' \
  -d '{"message":"Hello from workload","replicas":2}'

curl -fsS http://127.0.0.1:8080/instances
```

Creation happens in the background. Check `/instances` until both workers have
`ready: true`, then open one of their endpoint URLs. Press Ctrl-C in the app's
terminal to stop the app and its workers.

The example keeps its settings in memory and listens only on your computer. Its
[walkthrough](examples/control-plane/README.md) also covers changing messages,
scaling, expired permissions, and shutdown.

## Use workload in your app

You provide three things:

- A **definition** that turns your app's settings into a command or container to run.
- A **driver** that runs it locally, in Docker, or on Kubernetes.
- **Assignments** describing each instance you want to run, with its ID, settings,
  version, and permission deadline.

A `Manager` connects these pieces. After configuring it, the main calls are:

```go
// Supply the complete list of instances you want to run.
err = manager.Apply(ctx, workload.Snapshot{Assignments: assignments})

// Run the manager until its context is cancelled. Check the returned error.
err = manager.Run(ctx)

// From another goroutine, check each instance's state and ready endpoints.
observations := manager.Status()
```

These calls show the flow; [example_test.go](example_test.go) shows how to configure
and connect the pieces. You can call `Apply` and `Status` while `Run` is active.

Each assignment describes one running instance. To request three copies, provide
three assignments with different IDs. Keep an instance's ID stable across updates.
Change its revision or settings when you want workload to replace it.

`Apply` accepts the full list, rather than a set of changes. Leaving an instance
out requests that it stop; an empty list requests that everything stop. If your
app fails to fetch its settings, keep the previous list and its existing deadlines
instead of sending an empty list.

Workload is for long-running servers and workers. It restarts an assigned process
even if that process exits successfully. It is not a queue for one-time jobs.

## Choose how to run processes

| Driver | What you need |
| --- | --- |
| Local | Linux or macOS, the companion supervisor binary, and a private state directory |
| Docker | Access to a Docker daemon |
| Kubernetes | Access to a Kubernetes cluster |

For local execution, build the supervisor as shown above. Set
`LocalDriverConfig.SupervisorBinary` to its absolute path and `StateDirectory` to
an absolute directory owned by the app's user, with permissions `0700`. Keep this
directory between app restarts: it lets a new app process reconnect to workers
that are already running. Workers must run in the foreground and keep their child
processes in the same process group.

Definitions can describe multiple named TCP endpoints. Workload waits for all of
them to accept connections before reporting the instance as ready. A worker with
no endpoints is considered ready after it starts. When recovering a local process,
reuse the ports supplied to your definition's `Prepare` method.

You can request memory, CPU, and process-count limits. Docker supports all three;
Kubernetes supports memory and CPU. The local driver does not support these limits.
Unsupported requests return an error rather than being silently ignored.

## Keep control in your app

You do not need a separate scheduling service. Your app can load settings from a
file, a database, or an API and pass them to `Manager.Apply`.

If you prefer workload to poll your service, implement `ControlPlane` and use
`NewReconciler`. The [Go example](examples/control-plane/) demonstrates this approach.
Your app still owns storage, authentication, choosing machines, and deciding which
app process is allowed to manage each instance.

Every assignment passed to `Manager.Apply` needs a future `LeaseUntil`: the time
until which it has permission to run. Renew that permission by supplying a new
list with later deadlines. Renewal alone does not restart a worker. If permission
expires, the running manager stops the worker and removes its ready endpoints.

On restart, workload reconnects to matching processes or containers instead of
starting duplicates. It checks their identity and launch settings before reusing
them. Change the revision when changing referenced secrets or other external
configuration, and pin container image digests when you need a fixed image version.

Your app must ensure that two controllers cannot manage the same instance at once.
An expired lease or a lost connection does not prove that a remote worker has
stopped. Confirm termination, or prevent the old worker from acting, before moving
its work to another machine. For local execution, a supervisor crash blocks
automatic relaunch until you verify that its old workers have stopped. Do not
delete its state files just to force a restart.

## Shut down cleanly

Cancel the context passed to `Run`, then check its returned error. You can also call
`manager.Shutdown(ctx)` directly. It uses the configured stop timeout and your
context's deadline to limit how long cleanup can take.

If shutdown fails, retain ownership and retry `Shutdown`. Workload keeps track of
instances whose cleanup is incomplete and does not replace them while their
termination is uncertain. A manager cannot be restarted after shutdown; create a
new one for a new run.

For the full API rules, including custom drivers and distributed operation, see
the [assignment and lease contract](docs/assignment-lease-contract.md).

## Run the tests

```sh
go test -race ./...
go vet ./...
```

CI runs builds, formatting checks, vet, and race tests on Linux and macOS with
Go 1.25 and the latest stable Go version. It also tests against real Docker and
Kubernetes environments.

To run those integration tests locally, start Docker, then use:

```sh
scripts/integration-test.sh docker

# Also requires Kind and kubectl:
scripts/integration-test.sh kubernetes

# Run both:
scripts/integration-test.sh all
```

The scripts build their test image and clean up the resources they create.
Kubernetes tests use a separate, temporary Kind cluster rather than your current
Kubernetes context. Docker tests use your current Docker context unless you set
`WORKLOAD_TEST_DOCKER_CONTEXT`.
