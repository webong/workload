#!/usr/bin/env bash
set -euo pipefail

# All resources have a per-invocation image/network/cluster identity. No prune,
# global Docker-context switch or default kubeconfig mutation is performed.
mode="${1:-all}"
case "$mode" in docker|kubernetes|all) ;; *) printf 'usage: %s [docker|kubernetes|all]\n' "$0" >&2; exit 2 ;; esac
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
workload_temp="$(mktemp -d "${TMPDIR:-/tmp}/workload-integration.XXXXXXXX")"
run_id="wl-it-$(date +%s)-$$"
image="workload-integration:$run_id"
network="$run_id"
cluster="$run_id"
kubeconfig="$workload_temp/kubeconfig"
diagnostics="${WORKLOAD_TEST_ARTIFACT_DIR:-$workload_temp/diagnostics}"
mkdir -p "$diagnostics"
export DOCKER_CONTEXT="${WORKLOAD_TEST_DOCKER_CONTEXT:-$(docker context show)}"
network_created=false
cluster_created=false

cleanup() {
  status=$?
  trap - EXIT
  if [[ "$cluster_created" == true ]]; then
    if [[ "$status" != 0 ]]; then
      kind export logs "$diagnostics/kind-logs" --name "$cluster" || true
      kubectl --kubeconfig "$kubeconfig" get pods --all-namespaces -o wide >"$diagnostics/pods.log" 2>&1 || true
      kubectl --kubeconfig "$kubeconfig" get events --all-namespaces >"$diagnostics/events.log" 2>&1 || true
    fi
    KUBECONFIG="$kubeconfig" kind delete cluster --name "$cluster" || status=1
  fi
  # Only containers built from this invocation's unique test image are removed.
  ids="$(docker ps --all --quiet --filter "ancestor=$image")" || status=1
  if [[ -n "${ids:-}" ]]; then
    while IFS= read -r id; do
      if [[ "$status" != 0 ]]; then
        docker logs --tail 100 "$id" >"$diagnostics/docker-$id.log" 2>&1 || true
        docker inspect --format '{{json .State}}' "$id" >"$diagnostics/docker-$id-state.json" 2>&1 || true
      fi
      docker rm --force "$id" || status=1
    done <<< "$ids"
  fi
  if [[ "$network_created" == true ]]; then docker network rm "$network" || status=1; fi
  docker image rm "$image" >/dev/null 2>&1 || true
  if [[ "$status" == 0 ]]; then rm -rf "$workload_temp"; else printf 'Integration diagnostics: %s\n' "$workload_temp" >&2; fi
  exit "$status"
}
trap cleanup EXIT

docker info >/dev/null
architecture="$(docker info --format '{{.Architecture}}')"
case "$architecture" in aarch64|arm64) architecture=arm64 ;; x86_64|amd64) architecture=amd64 ;; *) printf 'Unsupported Docker architecture: %s\n' "$architecture" >&2; exit 1 ;; esac
CGO_ENABLED=0 GOOS=linux GOARCH="$architecture" go build -o "$workload_temp/server" ./testdata/integration/server
docker build --file testdata/integration/Dockerfile --tag "$image" "$workload_temp"
export WORKLOAD_TEST_IMAGE="$image"
export WORKLOAD_TEST_DOCKER_CONTEXT="$DOCKER_CONTEXT"

if [[ "$mode" == docker || "$mode" == all ]]; then
  docker network create "$network"
  network_created=true
  WORKLOAD_TEST_DOCKER=1 WORKLOAD_TEST_DOCKER_NETWORK="$network" go test -race -count=1 -timeout=12m -v ./integration -run '^TestDockerIntegration' 2>&1 | tee "$diagnostics/docker-test.log"
fi

if [[ "$mode" == kubernetes || "$mode" == all ]]; then
  command -v kind >/dev/null
  command -v kubectl >/dev/null
  cluster_created=true
  kind create cluster --name "$cluster" --kubeconfig "$kubeconfig" --wait 120s
  kind load docker-image "$image" --name "$cluster"
  WORKLOAD_TEST_KUBERNETES=1 WORKLOAD_TEST_KUBECONFIG="$kubeconfig" go test -race -count=1 -timeout=15m -v ./integration -run '^TestKubernetesIntegration' 2>&1 | tee "$diagnostics/kubernetes-test.log"
fi
