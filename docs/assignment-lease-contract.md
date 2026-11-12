# Runtime assignment and lease contract

Workload is an execution library. Consumers can implement their own control plane
in any language, keep desired state in-process, or adapt an existing scheduler.
This document describes the existing Go interfaces and their guarantees. It does
not define a required network protocol, persistence schema or PHP package.

## Integration choices

For host-driven reconciliation, call `Manager.Apply` with a complete `Snapshot`,
run `Manager.Run` in the host's lifecycle group, and read `Manager.Status`.
The host can fetch desired state through a database, API, queue or local code.
No separate control-plane server is required.

For a polling adapter, implement the existing interface and supply it to
`NewReconciler`:

```go
type ControlPlane interface {
    Assignments(context.Context, string, []string) ([]Spec, string, error)
    Report(context.Context, string, string, InstanceReport) error
}
```

`Assignments` receives the configured node ID and driver names. Its first result
is the complete assignment snapshot for that node. Its optional second result is
an informational placement leader ID; it is not an ownership or fencing token.
`Report` receives workload ID, assignment ID and an observation. The adapter owns
transport, authentication, persistence, idempotency and any durable ordering.
Reports may run concurrently during shutdown. All methods must honor cancellation.

## Assignment identity and desired state

A `Spec` represents one assigned instance. The host expands replica counts into
separate assignments before supplying them. Assignment IDs must be nonempty and
unique throughout one snapshot, including across targets managed by that runtime.
The host is responsible for preventing identity collisions across workers.

Prefer stable IDs for logical replicas when recovery/adoption is intended. Changing
an assignment ID removes the old assignment and requests a new one; workload does
not infer that the two IDs represent the same replica. A runtime PID or Pod UID
is an observed resource identity, not a suitable logical assignment ID.

The specification carries workload ID, kind, driver, desired state, positive
revision, configuration and lease deadline. The host configures execution targets
and credentials through the drivers. Revision/configuration changes cause
replacement; the runtime does not enforce increasing revisions or reject stale
updates from a consumer's data source. The consumer must impose that policy if
needed.

`Manager.Apply` validates and copies a complete snapshot before accepting it.
A malformed or duplicate snapshot is rejected without replacing the prior desired
state. Acceptance is asynchronous; inspect observations for actual readiness.
A valid empty snapshot requests stopping all managed assignments. A stopped
assignment is not launched. Do not turn a failed desired-state fetch into an empty
snapshot: preserve the prior snapshot and its original lease deadlines instead.

## Lease deadlines

Every assignment passed to `Manager.Apply` must have a future `LeaseUntil`.
Renewal requires an explicit new snapshot with a later deadline; storing or
re-reading an old snapshot does not extend its leases. Extending only a deadline
does not restart an otherwise unchanged instance.

The live manager wakes for lease expiry independently of its normal poll interval.
At expiry it begins stopping the instance, withdraws readiness, and does not
restart it from the expired snapshot. Stop can consume its configured shutdown
budget or fail; expiry is not a promise that the process was physically terminated
at that exact instant.

The lower-level `Reconciler` also supports the original node-refresh timeout:
unavailable assignment snapshots eventually stop managed instances after
`CoordinationLease`. Specs with nonzero assignment deadlines are additionally
subject to those individual deadlines. The low-level interface permits zero
assignment deadlines for that legacy mode; `Manager.Apply` does not.

The runtime consumes deadlines in the host's clock domain. It does not estimate
remote clock offset or implement a lease-renewal wire protocol. A network adapter
must translate authoritative remote leases conservatively, account for transit
time and clock uncertainty, and refuse expired responses. Request retries and
transport failures must not fabricate fresh permission to run.

## Ownership and failover

The host must establish authoritative ownership before running reconciliation or
invoking recovery cleanup. Node IDs and resource labels identify resources; they
are not authentication credentials or distributed fencing tokens.

The Go runtime does not implement worker sessions, node/assignment epochs, a
placement algorithm, a durable lease database or automatic cross-node takeover.
Consumers may use those mechanisms behind their adapter, but none is mandated by
the library interface. For distributed operation, the consumer must reject stale
actors and prevent conflicting owners through its own authoritative system.

A failed heartbeat or expired lease does not prove that a partitioned worker or
workload has stopped. Before transferring ownership, the host must either confirm
termination or provide suitable fencing. Same-node recovery likewise requires
ensuring that an old controller cannot interfere with the adopting controller.
The choice and verification of that mechanism belong to the consumer.

## Observations and termination

Observations distinguish starting, running, stopping, stopped and failure states.
`Manager.Status` returns copied observations; only running observations advertise
ready endpoints. A foreground worker may be running without any listeners.
Stored observations are not a durable acceptance or exactly-once delivery record.

`Process.Done` signals observed exit. `Process.Stop` must also confirm cleanup;
when cleanup is uncertain, the runtime retains the handle and reports the failure.
A failed or stop-failed report is not a stopped acknowledgement. A control plane
must not use it to justify unsafe reassignment or advertise an old endpoint.

On host shutdown, cancel `Run` or call `Shutdown` and inspect the returned error.
Release upstream ownership only after confirmed cleanup or independent fencing.
An unsuccessful shutdown retains unresolved instances for retry. The consumer
owns any durable retry/report outbox and its policy for unresolved ownership.

## Consumer checklist

- Supply complete, validated desired state and unique logical assignment IDs.
- Renew permission explicitly and retain existing deadlines on fetch failure.
- Configure driver access and resource scopes for the intended target.
- Establish exclusive ownership before executing or cleaning up resources.
- Authenticate and fence stale actors where distributed operation requires it.
- Treat observations as observations and confirm termination before failover.

The existing Manager, Reconciler and real-runtime test suites exercise snapshot
validation, lease renewal/expiry, replacement, resource recovery and retryable
shutdown. No PHP runtime, Composer package or database service is required.
