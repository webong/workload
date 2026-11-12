package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/webong/workload"
)

func request(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
	return response
}
func TestControlPlaneOwnsDesiredStateAndLeaseRenewal(t *testing.T) {
	now := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	plane := newControlPlane("demo-node", 10*time.Second)
	plane.now = func() time.Time { return now }
	api := plane.handler()
	response := request(t, api, http.MethodPut, "/workloads/hello", `{"message":"hello","replicas":2}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("create: %d %s", response.Code, response.Body)
	}
	first, _, err := plane.Assignments(context.Background(), "demo-node", []string{workload.DriverLocal})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].AssignmentID != "hello/0" || first[1].AssignmentID != "hello/1" || first[0].Revision != 1 || !first[0].LeaseUntil.Equal(now.Add(10*time.Second)) {
		t.Fatalf("wrong assignments: %+v", first)
	}
	first[0].Configuration[0] = '!'
	request(t, api, http.MethodPost, "/control-plane/pause", "")
	if _, _, err := plane.Assignments(context.Background(), "demo-node", []string{workload.DriverLocal}); err == nil {
		t.Fatal("paused fetch must fail, not return an empty successful snapshot")
	}
	now = now.Add(20 * time.Second)
	request(t, api, http.MethodPost, "/control-plane/resume", "")
	next, _, err := plane.Assignments(context.Background(), "demo-node", []string{workload.DriverLocal})
	if err != nil {
		t.Fatal(err)
	}
	if next[0].AssignmentID != "hello/0" || !json.Valid(next[0].Configuration) || !next[0].LeaseUntil.Equal(now.Add(10*time.Second)) {
		t.Fatalf("snapshot was aliased or not explicitly renewed: %+v", next)
	}
}
func TestControlPlaneScalesWithoutRestartingHealthyReplicas(t *testing.T) {
	plane := newControlPlane("demo-node", 10*time.Second)
	api := plane.handler()
	for _, body := range []string{`{"message":"one","replicas":2}`, `{"message":"one","replicas":1}`} {
		if response := request(t, api, http.MethodPut, "/workloads/hello", body); response.Code != http.StatusAccepted {
			t.Fatal(response.Body.String())
		}
	}
	assignments, _, _ := plane.Assignments(context.Background(), "demo-node", []string{workload.DriverLocal})
	if len(assignments) != 1 || assignments[0].Revision != 1 {
		t.Fatalf("scaling changed execution revision: %+v", assignments)
	}
	request(t, api, http.MethodPut, "/workloads/hello", `{"message":"two","replicas":1}`)
	assignments, _, _ = plane.Assignments(context.Background(), "demo-node", []string{workload.DriverLocal})
	if assignments[0].Revision != 2 {
		t.Fatal("configuration change did not advance revision")
	}
	if response := request(t, api, http.MethodPut, "/workloads/hello", `{"message":"bad","replicas":5}`); response.Code != http.StatusBadRequest {
		t.Fatal("invalid update accepted")
	}
	request(t, api, http.MethodDelete, "/workloads/hello", "")
	assignments, _, _ = plane.Assignments(context.Background(), "demo-node", []string{workload.DriverLocal})
	if len(assignments) != 0 {
		t.Fatal("delete left desired assignments")
	}
}
