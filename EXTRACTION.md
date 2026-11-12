# Extraction provenance

Prepared from Gateway commit `9db868b86a5a67817cfd52f6365a666cea035b86`.
Original sources: `pkg/spinner/provision` and
`mod/arranger/integration/provision.go`. The original MIT attribution is retained.
No Gateway files or integration points have been changed.

This is an independent module without a tagged release. Its contracts have intentionally
changed while preparing the extraction; it is not a drop-in replacement.

## Completed interface work

- Explicit `Manager.Apply`, `Run`, `Status` and `Shutdown` interface, independent
  of a control-plane transport. Snapshots and observations are copied.
- Per-assignment lease deadlines with validation, explicit renewal, expiry wakeup
  and no implicit renewal of stored snapshots.
- Context-aware definition validation/preparation with recoverable named-port
  placement; drivers derive assignment identity directly from the specification.
- Multiple named TCP endpoints and foreground workers without listeners.
- Explicit memory/CPU/PID limit requests, enforcement where supported and errors
  for unsupported ceilings. One assignment means one instance; no ignored replica
  multiplier or product hostname/path fields.
- Starting, ready, stopping and failure observations; recovery errors propagate
  to callers rather than disappearing into an optional logger.

## Completed recovery work

- Collision-resistant names scoped to node, target and assignment; hashed label
  identities support arbitrary caller strings within Kubernetes label limits.
- Fingerprints cover revision, configuration, launch and driver settings.
  Adoption checks observed execution fields, including requested limits.
- Stale owned resources are replaced; foreign resources remain untouched.
  Known managed handles retain their normal graceful stop path during cleanup.
- Pod deletion carries an atomic UID precondition and confirms disappearance.
  Transport/API failures never stand in for confirmed process termination.
- Ambiguous creation and readiness failures retain a cleanup handle.
- Local processes run under a separate, locked companion supervisor so worker
  exit permits adoption through a private socket. Durable journals and terminal
  receipts preserve ownership and shutdown confirmation without trusting PID files.

## Completed shutdown work

- Run returns shutdown/recovery errors. Shutdown has a total deadline and bounded
  concurrency, is terminal and supports retries of unresolved resources.
- Failed stops withdraw endpoints, retain handles and block duplicate replacement.
- Local process groups receive TERM then KILL, with child reaping. Successful local
  shutdown can be confirmed from a durable receipt after a lost RPC reply.
- Cleanup also discovers resources that were left behind by interrupted creation.

## Integration contract and validation limits

Durable placement, exclusive worker ownership and cross-node fencing remain host
responsibilities. Live-manager leases are not a distributed crash watchdog.
An orphaned local supervisor journal requires verified termination before manual
removal; guessing from a PID is intentionally unsupported. Long-running processes
are supported; durable one-shot job acceptance/replay is outside this module.
Local supervision requires Linux/macOS and the companion executable. Unsupported
resource ceilings return errors. Existing Gateway labels are never auto-migrated.

The regression suite uses actual local worker/supervisor subprocesses and private
sockets, and stateful Docker/Kubernetes command fakes. Live Docker and Kubernetes
validation is still needed before deployment. The module is published independently at `github.com/webong/workload`; it has
not been wired into Gateway.
