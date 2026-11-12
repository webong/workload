# Workload

Workload executes host-supplied assignments for long-running processes. Consumers
own desired state, placement and any control-plane implementation.

## Language

**Definition**: A technology-specific description of how to validate and prepare
an executable launch.
_Avoid_: control plane

**Specification**: The requested execution settings and desired state of one
assigned instance.

**Assignment**: The host's request to manage one instance under an identity and
lease deadline.
_Avoid_: replica count

**Instance**: A concrete process, container or Pod executing an assignment.
_Avoid_: assignment identity

**Revision**: A host-supplied positive version of an execution specification.
_Avoid_: ownership epoch

**Node**: The host-selected identity under which a runtime manages its resources.
_Avoid_: worker session

**Target**: An execution scope configured by the host for a driver.
_Avoid_: tenant

**Lease**: The host-supplied deadline after which the live runtime must stop
managing an assignment as running.
_Avoid_: proof of termination

**Observation**: The runtime's report of an assigned instance's state and named
endpoints.
_Avoid_: durable desired state

**Fencing**: A host-provided mechanism that prevents an old controller or workload
from acting after ownership changes.
_Avoid_: lease expiry
