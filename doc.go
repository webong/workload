// Package workload manages assigned long-running processes independently of a
// product control plane or router. Manager accepts explicit leased snapshots,
// exposes named endpoint observations, and coordinates bounded, retryable shutdown.
// Definitions prepare technology-specific launches; local, Docker and Kubernetes
// drivers execute and recover them. The host supplies durable exclusive worker
// ownership and cross-node fencing. Local recovery uses the workload-supervisor
// companion executable on Linux or macOS.
package workload
