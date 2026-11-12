# Consumers own the control plane

Workload remains an execution library: consumers supply desired state, assignment
placement and authoritative ownership through snapshots or the ControlPlane
interface. We defer the proposed PHP control-plane package so the runtime does
not prescribe a framework, database, transport, session protocol or failover
policy; an embedded host and a distributed scheduler can use the same execution
interface while providing the ownership guarantees appropriate to their system.
