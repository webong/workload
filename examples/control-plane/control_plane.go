package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/webong/workload"
)

// This is application code, not another workload package: one node, one owner,
// in-memory desired state, and a loopback HTTP API. Replace this storage/policy
// with your own system while keeping the workload.ControlPlane interface.
type controlPlane struct {
	mu        sync.RWMutex
	nodeID    string
	lease     time.Duration
	now       func() time.Time
	paused    bool
	desired   map[string]desiredWorkload
	revisions map[string]int64
	reports   map[string]workload.InstanceReport
	leases    map[string]assignmentLease
}
type desiredWorkload struct {
	ID       string `json:"id"`
	Message  string `json:"message"`
	Replicas int    `json:"replicas"`
	Revision int64  `json:"revision"`
}
type assignmentLease struct {
	revision int64
	until    time.Time
}
type messageConfiguration struct {
	Message string `json:"message"`
}

func newControlPlane(nodeID string, lease time.Duration) *controlPlane {
	return &controlPlane{nodeID: nodeID, lease: lease, now: time.Now, desired: map[string]desiredWorkload{}, revisions: map[string]int64{}, reports: map[string]workload.InstanceReport{}, leases: map[string]assignmentLease{}}
}
func (c *controlPlane) Assignments(ctx context.Context, nodeID string, drivers []string) ([]workload.Spec, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if nodeID != c.nodeID || !slices.Contains(drivers, workload.DriverLocal) {
		return nil, "", errors.New("this example only assigns its local node")
	}
	if c.paused {
		return nil, "", errors.New("example control plane is paused")
	}
	ids := make([]string, 0, len(c.desired))
	for id := range c.desired {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	until := c.now().Add(c.lease)
	specs := []workload.Spec{}
	for _, id := range ids {
		desired := c.desired[id]
		configuration, _ := json.Marshal(messageConfiguration{desired.Message})
		for replica := 0; replica < desired.Replicas; replica++ {
			assignmentID := id + "/" + strconv.Itoa(replica)
			specs = append(specs, workload.Spec{ID: id, AssignmentID: assignmentID, Type: "message-server", Driver: workload.DriverLocal, DesiredState: workload.DesiredRunning, Revision: desired.Revision, Configuration: append(json.RawMessage(nil), configuration...), LeaseUntil: until})
			c.leases[assignmentID] = assignmentLease{desired.Revision, until}
		}
	}
	return specs, "", nil
}
func (c *controlPlane) Report(ctx context.Context, workloadID, assignmentID string, report workload.InstanceReport) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if report.NodeID != c.nodeID || report.WorkloadID != workloadID || report.AssignmentID != assignmentID {
		return errors.New("report identity does not match its assignment")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if previous, ok := c.reports[assignmentID]; ok && previous.Revision > report.Revision {
		return errors.New("stale revision report")
	}
	report.Endpoints = append([]workload.Endpoint(nil), report.Endpoints...)
	if report.State != "running" {
		report.Endpoints = nil
	}
	c.reports[assignmentID] = report
	return nil
}

type endpointView struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port int    `json:"port"`
	URL  string `json:"url"`
}
type instanceView struct {
	AssignmentID string         `json:"assignment_id"`
	WorkloadID   string         `json:"workload_id"`
	Revision     int64          `json:"revision"`
	State        string         `json:"state"`
	PID          int            `json:"pid"`
	Ready        bool           `json:"ready"`
	LeaseUntil   time.Time      `json:"lease_until"`
	Endpoints    []endpointView `json:"endpoints"`
	Error        string         `json:"error,omitempty"`
}

func (c *controlPlane) instances() []instanceView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := c.now()
	result := make([]instanceView, 0, len(c.reports))
	for id, report := range c.reports {
		lease := c.leases[id]
		desired, exists := c.desired[report.WorkloadID]
		ready := exists && report.State == "running" && desired.Revision == report.Revision && lease.revision == report.Revision && now.Before(lease.until)
		// Scale-down withdraws endpoints immediately while the reconciler performs
		// actual termination. Reports still show that asynchronous transition.
		slot := strings.TrimPrefix(id, report.WorkloadID+"/")
		index, err := strconv.Atoi(slot)
		ready = ready && err == nil && index >= 0 && index < desired.Replicas
		view := instanceView{AssignmentID: id, WorkloadID: report.WorkloadID, Revision: report.Revision, State: report.State, PID: report.PID, Ready: ready, LeaseUntil: lease.until, Endpoints: []endpointView{}, Error: report.Error}
		if ready {
			for _, endpoint := range report.Endpoints {
				view.Endpoints = append(view.Endpoints, endpointView{endpoint.Name, endpoint.Host, endpoint.Port, "http://" + net.JoinHostPort(endpoint.Host, strconv.Itoa(endpoint.Port))})
			}
		}
		result = append(result, view)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].AssignmentID < result[j].AssignmentID })
	return result
}

var validID = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

func (c *controlPlane) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /workloads", func(w http.ResponseWriter, r *http.Request) {
		c.mu.RLock()
		items := make([]desiredWorkload, 0, len(c.desired))
		for _, item := range c.desired {
			items = append(items, item)
		}
		c.mu.RUnlock()
		sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
		writeJSON(w, http.StatusOK, items)
	})
	mux.HandleFunc("PUT /workloads/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !validID.MatchString(id) {
			writeError(w, http.StatusBadRequest, "invalid workload ID")
			return
		}
		var input struct {
			Message  string `json:"message"`
			Replicas *int   `json:"replicas"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeError(w, http.StatusBadRequest, "expected message and replicas JSON")
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			writeError(w, http.StatusBadRequest, "expected exactly one JSON object")
			return
		}
		if strings.TrimSpace(input.Message) == "" || len(input.Message) > 4096 || input.Replicas == nil || *input.Replicas < 0 || *input.Replicas > 4 {
			writeError(w, http.StatusBadRequest, "message must be 1–4096 bytes and replicas 0–4")
			return
		}
		c.mu.Lock()
		previous, exists := c.desired[id]
		if !exists && len(c.desired) >= 8 {
			c.mu.Unlock()
			writeError(w, http.StatusConflict, "this example allows at most eight workloads")
			return
		}
		revision := c.revisions[id]
		if !exists || previous.Message != input.Message {
			revision++
		}
		item := desiredWorkload{ID: id, Message: input.Message, Replicas: *input.Replicas, Revision: revision}
		c.desired[id] = item
		c.revisions[id] = revision
		c.mu.Unlock()
		writeJSON(w, http.StatusAccepted, item)
	})
	mux.HandleFunc("DELETE /workloads/{id}", func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		id := r.PathValue("id")
		if _, exists := c.desired[id]; !exists {
			c.mu.Unlock()
			writeError(w, http.StatusNotFound, "workload not found")
			return
		}
		delete(c.desired, id)
		c.mu.Unlock()
		writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "desired_state": "stopped"})
	})
	mux.HandleFunc("GET /instances", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, http.StatusOK, c.instances()) })
	mux.HandleFunc("GET /control-plane", func(w http.ResponseWriter, r *http.Request) {
		c.mu.RLock()
		defer c.mu.RUnlock()
		writeJSON(w, http.StatusOK, map[string]any{"node_id": c.nodeID, "paused": c.paused, "lease_seconds": c.lease.Seconds()})
	})
	for _, action := range []string{"pause", "resume"} {
		mux.HandleFunc("POST /control-plane/"+action, func(w http.ResponseWriter, r *http.Request) {
			c.mu.Lock()
			c.paused = action == "pause"
			c.mu.Unlock()
			writeJSON(w, http.StatusOK, map[string]bool{"paused": action == "pause"})
		})
	}
	return mux
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

var _ workload.ControlPlane = (*controlPlane)(nil)
