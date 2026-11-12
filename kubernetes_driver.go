package workload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KubernetesDriverConfig struct {
	TargetID             string
	Context              string
	Kubeconfig           string
	KubectlBinary        string
	NodeID               string
	Namespace            string
	ServiceAccount       string
	EnvironmentSecret    string
	EnvironmentConfigMap string
	ImagePullPolicy      string
	RouteMode            string
	StartTimeout         time.Duration
	StopTimeout          time.Duration
}

type KubernetesDriver struct {
	config KubernetesDriverConfig
	runner commandRunner
}

func NewKubernetesDriver(config KubernetesDriverConfig) (*KubernetesDriver, error) {
	return newKubernetesDriver(config, execCommandRunner{})
}

func newKubernetesDriver(config KubernetesDriverConfig, runner commandRunner) (*KubernetesDriver, error) {
	if strings.TrimSpace(config.KubectlBinary) == "" {
		return nil, fmt.Errorf("provisioning Kubernetes kubectl binary is required")
	}
	if strings.TrimSpace(config.NodeID) == "" {
		return nil, fmt.Errorf("provisioning Kubernetes node ID is required")
	}
	if strings.TrimSpace(config.Namespace) == "" {
		return nil, fmt.Errorf("provisioning Kubernetes namespace is required")
	}
	if config.ImagePullPolicy != "Always" && config.ImagePullPolicy != "IfNotPresent" && config.ImagePullPolicy != "Never" {
		return nil, fmt.Errorf("provisioning Kubernetes image pull policy must be Always, IfNotPresent, or Never")
	}
	if config.RouteMode != "pod-ip" && config.RouteMode != "port-forward" {
		return nil, fmt.Errorf("provisioning Kubernetes route mode must be pod-ip or port-forward")
	}
	if config.StartTimeout <= 0 || config.StopTimeout <= 0 {
		return nil, fmt.Errorf("provisioning Kubernetes timeouts must be positive")
	}
	if runner == nil {
		return nil, fmt.Errorf("provisioning Kubernetes command runner is required")
	}

	runner = prefixedCommandRunner{runner: runner, arguments: config.connectionArguments()}
	return &KubernetesDriver{config: config, runner: runner}, nil
}

func (c KubernetesDriverConfig) connectionArguments() []string {
	var args []string
	if c.Kubeconfig != "" {
		args = append(args, "--kubeconfig", c.Kubeconfig)
	}
	if c.Context != "" {
		args = append(args, "--context", c.Context)
	}
	return args
}

func (d *KubernetesDriver) selector() string {
	selector := "workload.webong.dev/node=" + runtimeLabelValue(d.config.NodeID)
	selector += ",workload.webong.dev/target=" + runtimeLabelValue(d.config.TargetID)
	return selector
}

type kubernetesPod struct {
	Metadata struct {
		Name              string            `json:"name"`
		UID               string            `json:"uid"`
		Labels            map[string]string `json:"labels"`
		DeletionTimestamp *time.Time        `json:"deletionTimestamp"`
	} `json:"metadata"`
	Spec   json.RawMessage `json:"spec"`
	Status struct {
		Phase             string `json:"phase"`
		PodIP             string `json:"podIP"`
		ContainerStatuses []struct {
			Ready bool `json:"ready"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

func (d *KubernetesDriver) get(ctx context.Context, name string) (kubernetesPod, bool, error) {
	out, err := d.runner.Output(ctx, nil, d.config.KubectlBinary, "--namespace", d.config.Namespace, "get", "pod", name, "--ignore-not-found=true", "--output=json")
	if err != nil {
		return kubernetesPod{}, false, err
	}
	if strings.TrimSpace(out) == "" {
		return kubernetesPod{}, true, nil
	}
	var p kubernetesPod
	err = json.Unmarshal([]byte(out), &p)
	return p, false, err
}

func (d *KubernetesDriver) owns(labels map[string]string) bool {
	return labels["workload.webong.dev/node"] == runtimeLabelValue(d.config.NodeID) &&
		labels["workload.webong.dev/target"] == runtimeLabelValue(d.config.TargetID)
}

func (d *KubernetesDriver) PrepareDesired(ctx context.Context, desired map[string]Spec) error {
	out, err := d.runner.Output(ctx, nil, d.config.KubectlBinary, "--namespace", d.config.Namespace, "get", "pods", "--selector", d.selector(), "--output=json")
	if err != nil {
		return err
	}
	var list struct {
		Items []kubernetesPod `json:"items"`
	}
	if err = json.Unmarshal([]byte(out), &list); err != nil {
		return err
	}
	for _, pod := range list.Items {
		if !d.owns(pod.Metadata.Labels) {
			continue
		}
		keep := false
		for id := range desired {
			if runtimeLabelValue(id) == pod.Metadata.Labels["workload.webong.dev/instance"] {
				keep = true
			}
		}
		if keep {
			continue
		}
		p := d.process(pod, nil)
		if err = p.Stop(ctx); err != nil {
			return fmt.Errorf("remove stale pod: %w", err)
		}
	}
	return nil
}
func (d *KubernetesDriver) process(pod kubernetesPod, endpoints []Endpoint) *kubernetesProcess {
	p := &kubernetesProcess{runner: d.runner, binary: d.config.KubectlBinary, namespace: d.config.Namespace, name: pod.Metadata.Name, uid: pod.Metadata.UID, endpoints: endpoints, stopTimeout: d.config.StopTimeout, done: make(chan struct{})}
	if len(endpoints) > 0 {
		p.host = endpoints[0].Host
		p.port = endpoints[0].Port
	}
	return p
}
func (d *KubernetesDriver) Start(ctx context.Context, spec Spec, definition Definition) (Process, error) {
	instanceID := spec.AssignmentID
	ctx, cancel := context.WithTimeout(ctx, d.config.StartTimeout)
	defer cancel()
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if spec.Driver != DriverKubernetes {
		return nil, fmt.Errorf("Kubernetes cannot run driver %q", spec.Driver)
	}
	launch, err := definition.Prepare(ctx, spec, Placement{Driver: DriverKubernetes, BindHost: "0.0.0.0"})
	if err != nil {
		return nil, err
	}
	if err = launch.validate(true); err != nil {
		return nil, err
	}
	if launch.Limits.PIDs != 0 {
		return nil, fmt.Errorf("Kubernetes pod PID limits are not supported by this driver")
	}
	name := runtimeResourceName(identityKey(d.config.NodeID, d.config.TargetID, instanceID))
	manifest, err := kubernetesPodManifest(d.config, spec, instanceID, name, launch)
	if err != nil {
		return nil, err
	}
	fingerprint := launchFingerprint(spec, launch, d.config)
	pod, absent, err := d.get(ctx, name)
	if err != nil {
		return nil, err
	}
	if !absent {
		if pod.Metadata.Name != name || pod.Metadata.UID == "" || !d.owns(pod.Metadata.Labels) || pod.Metadata.Labels["workload.webong.dev/instance"] != runtimeLabelValue(instanceID) {
			return nil, fmt.Errorf("pod ownership conflict")
		}
		if pod.Metadata.Labels["workload.webong.dev/fingerprint"] != fingerprint || !podSpecMatches(manifest, pod.Spec) || pod.Metadata.DeletionTimestamp != nil || pod.Status.Phase == "Failed" || pod.Status.Phase == "Succeeded" {
			if err = d.process(pod, nil).Stop(ctx); err != nil {
				return nil, fmt.Errorf("replace stale pod: %w", err)
			}
			absent = true
		}
	}
	if absent {
		out, createErr := d.runner.Output(ctx, manifest, d.config.KubectlBinary, "--namespace", d.config.Namespace, "create", "--filename", "-", "--output=json")
		if createErr != nil {
			return d.pending(name, instanceID, fingerprint), fmt.Errorf("create workload pod: %w", createErr)
		}
		if err = json.Unmarshal([]byte(out), &pod); err != nil {
			return d.pending(name, instanceID, fingerprint), fmt.Errorf("decode created pod: %w", err)
		}
		if pod.Metadata.UID == "" || pod.Metadata.Name != name || !d.owns(pod.Metadata.Labels) || pod.Metadata.Labels["workload.webong.dev/fingerprint"] != fingerprint {
			return d.pending(name, instanceID, fingerprint), fmt.Errorf("created pod identity does not match launch")
		}
	}
	p := d.process(pod, nil)
	// Retain the handle even if readiness fails; the manager must clean it up.
	ready, err := d.waitForReady(ctx, name, p.uid, len(launch.endpoints()) > 0)
	if err != nil {
		return p, err
	}
	endpoints := []Endpoint{}
	for _, e := range launch.endpoints() {
		endpoint := Endpoint{e.Name, ready.Status.PodIP, e.Port}
		if d.config.RouteMode == "port-forward" {
			forward, err := d.startPortForward(name, e.Port)
			if err != nil {
				return p, err
			}
			p.forwards = append(p.forwards, forward)
			endpoint.Host = forward.Host()
			endpoint.Port = forward.Port()
		}
		endpoints = append(endpoints, endpoint)
	}
	p.endpoints = endpoints
	if len(endpoints) > 0 {
		p.host = endpoints[0].Host
		p.port = endpoints[0].Port
	}
	go p.watch()
	if err = waitForListener(ctx, p); err != nil {
		return p, err
	}
	return p, nil
}
func (d *KubernetesDriver) waitForReady(ctx context.Context, name, uid string, needsEndpoint bool) (kubernetesPod, error) {
	for {
		p, absent, err := d.get(ctx, name)
		if err == nil {
			if absent {
				return p, fmt.Errorf("pod disappeared during startup")
			}
			if p.Metadata.UID != uid {
				return p, fmt.Errorf("pod changed identity during startup")
			}
			if p.Status.Phase == "Failed" || p.Status.Phase == "Succeeded" {
				return p, fmt.Errorf("pod entered terminal phase %s", p.Status.Phase)
			}
			ready := len(p.Status.ContainerStatuses) > 0
			for _, c := range p.Status.ContainerStatuses {
				ready = ready && c.Ready
			}
			if p.Status.Phase == "Running" && ready && (!needsEndpoint || p.Status.PodIP != "") {
				return p, nil
			}
		}
		select {
		case <-ctx.Done():
			return kubernetesPod{}, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// Compare every field we control, while tolerating API defaults and admission
// additions outside those fields. Array lengths prevent hidden extra containers.
func podSpecMatches(manifest []byte, actual json.RawMessage) bool {
	var expected struct {
		Spec any `json:"spec"`
	}
	var got any
	if json.Unmarshal(manifest, &expected) != nil || json.Unmarshal(actual, &got) != nil {
		return false
	}
	return containsFields(expected.Spec, got)
}
func containsFields(expected, actual any) bool {
	switch e := expected.(type) {
	case map[string]any:
		a, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range e {
			if k == "resources" {
				if !resourceFieldsMatch(v, a[k]) {
					return false
				}
				continue
			}
			if !containsFields(v, a[k]) {
				return false
			}
		}
		return true
	case []any:
		a, ok := actual.([]any)
		if len(e) == 0 && actual == nil {
			return true
		}
		if !ok || len(a) != len(e) {
			return false
		}
		for i := range e {
			if !containsFields(e[i], a[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(expected, actual)
	}
}

func (d *KubernetesDriver) startPortForward(name string, remotePort int) (*localProcess, error) {
	host := "127.0.0.1"
	port, err := availablePort(host)
	if err != nil {
		return nil, err
	}
	arguments := append(d.config.connectionArguments(),
		"--namespace", d.config.Namespace,
		"port-forward", "pod/"+name,
		"--address="+host,
		strconv.Itoa(port)+":"+strconv.Itoa(remotePort),
	)
	command := exec.Command(d.config.KubectlBinary, arguments...)
	configureProcessGroup(command)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return nil, err
	}
	process := &localProcess{
		command: command,
		host:    host,
		port:    port,
		done:    make(chan struct{}),
	}
	go process.wait()

	return process, nil
}

func kubernetesPodManifest(config KubernetesDriverConfig, spec Spec, instanceID, name string, launch LaunchSpec) ([]byte, error) {
	environment := make([]map[string]string, 0)
	for _, variable := range launch.Environment {
		key, value, _ := strings.Cut(variable, "=")
		environment = append(environment, map[string]string{"name": key, "value": value})
	}
	command := append([]string{launch.Executable}, launch.Arguments...)
	containerName := launch.ContainerName
	if containerName == "" {
		containerName = "server"
	}
	ports := []map[string]any{}
	for _, e := range launch.endpoints() {
		ports = append(ports, map[string]any{"name": "p-" + runtimeLabelValue(e.Name)[:13], "containerPort": e.Port, "protocol": "TCP"})
	}
	container := map[string]any{
		"name":            containerName,
		"image":           launch.Image,
		"imagePullPolicy": config.ImagePullPolicy,
		"command":         command,
		"env":             environment,
		"ports":           ports,
	}
	if launch.WorkingDirectory != "" {
		container["workingDir"] = launch.WorkingDirectory
	}
	if len(ports) > 0 {
		container["readinessProbe"] = map[string]any{"tcpSocket": map[string]any{"port": launch.endpoints()[0].Port}, "periodSeconds": 1, "timeoutSeconds": 1}
	}
	limits := map[string]string{}
	if launch.Limits.MemoryBytes > 0 {
		limits["memory"] = strconv.FormatInt(launch.Limits.MemoryBytes, 10)
	}
	if launch.Limits.MilliCPU > 0 {
		limits["cpu"] = strconv.FormatInt(launch.Limits.MilliCPU, 10) + "m"
	}
	if len(limits) > 0 {
		container["resources"] = map[string]any{"limits": limits}
	}

	environmentFrom := make([]map[string]any, 0, 2)
	if config.EnvironmentConfigMap != "" {
		environmentFrom = append(environmentFrom, map[string]any{"configMapRef": map[string]string{"name": config.EnvironmentConfigMap}})
	}
	if config.EnvironmentSecret != "" {
		environmentFrom = append(environmentFrom, map[string]any{"secretRef": map[string]string{"name": config.EnvironmentSecret}})
	}
	if len(environmentFrom) > 0 {
		container["envFrom"] = environmentFrom
	}
	podSpec := map[string]any{
		"restartPolicy":                 "Never",
		"terminationGracePeriodSeconds": int64(math.Ceil(config.StopTimeout.Seconds())),
		"containers":                    []map[string]any{container},
	}
	if config.ServiceAccount != "" {
		podSpec["serviceAccountName"] = config.ServiceAccount
	}
	manifest := map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name": name,
			"labels": map[string]string{
				"app.kubernetes.io/name":          "workload-provisioning",
				"app.kubernetes.io/managed-by":    "workload",
				"workload.webong.dev/type":        runtimeLabelValue(spec.Type),
				"workload.webong.dev/server":      runtimeLabelValue(spec.ID),
				"workload.webong.dev/instance":    runtimeLabelValue(instanceID),
				"workload.webong.dev/node":        runtimeLabelValue(config.NodeID),
				"workload.webong.dev/fingerprint": launchFingerprint(spec, launch, config),
			},
		},
		"spec": podSpec,
	}
	manifest["metadata"].(map[string]any)["labels"].(map[string]string)["workload.webong.dev/target"] = runtimeLabelValue(config.TargetID)

	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("encode provisioned Kubernetes pod: %w", err)
	}

	return encoded, nil
}

type kubernetesProcess struct {
	endpoints                          []Endpoint
	runner                             commandRunner
	binary, namespace, name, uid, host string
	port                               int
	forwards                           []*localProcess
	stopTimeout                        time.Duration
	done                               chan struct{}
	doneOnce                           sync.Once
	errMu                              sync.RWMutex
	err                                error
	health                             error
	stopMu                             sync.Mutex
}

func (p *kubernetesProcess) timeout() time.Duration {
	if p.stopTimeout <= 0 {
		return time.Second
	}
	return p.stopTimeout
}
func (p *kubernetesProcess) get(ctx context.Context) (kubernetesPod, bool, error) {
	return (&KubernetesDriver{config: KubernetesDriverConfig{KubectlBinary: p.binary, Namespace: p.namespace}, runner: p.runner}).get(ctx, p.name)
}
func (p *kubernetesProcess) watch() {
	// A forwarder belongs to this handle, even after its Pod vanishes or changes
	// identity. Release local helpers on every terminal observation; never leave
	// them holding the host's output pipes open after the handle is retired.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), p.timeout())
		defer cancel()
		if err := p.stopForwards(ctx); err != nil {
			p.setHealth(fmt.Errorf("stop port-forward: %w", err))
		}
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
		}
		for _, f := range p.forwards {
			select {
			case <-f.Done():
				p.setHealth(fmt.Errorf("port-forward exited"))
			default:
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), p.timeout())
		pod, absent, err := p.get(ctx)
		cancel()
		if err != nil {
			p.setHealth(err)
			continue
		}
		if absent {
			p.finish(nil)
			return
		}
		if pod.Metadata.UID != p.uid {
			p.setHealth(fmt.Errorf("pod changed identity"))
			return
		}
		if pod.Status.Phase == "Succeeded" {
			p.finish(nil)
			return
		}
		if pod.Status.Phase == "Failed" {
			p.finish(fmt.Errorf("pod failed"))
			return
		}
	}
}
func (p *kubernetesProcess) setHealth(err error) { p.errMu.Lock(); p.health = err; p.errMu.Unlock() }
func (p *kubernetesProcess) Health() error       { p.errMu.RLock(); defer p.errMu.RUnlock(); return p.health }
func (p *kubernetesProcess) finish(err error) {
	p.doneOnce.Do(func() { p.errMu.Lock(); p.err = err; p.errMu.Unlock(); close(p.done) })
}
func (p *kubernetesProcess) PID() int              { return 0 }
func (p *kubernetesProcess) Host() string          { return p.host }
func (p *kubernetesProcess) Port() int             { return p.port }
func (p *kubernetesProcess) Done() <-chan struct{} { return p.done }
func (p *kubernetesProcess) Err() error            { p.errMu.RLock(); defer p.errMu.RUnlock(); return p.err }
func (p *kubernetesProcess) Endpoints() []Endpoint { return append([]Endpoint(nil), p.endpoints...) }
func (p *kubernetesProcess) Stop(ctx context.Context) (result error) {
	p.stopMu.Lock()
	defer p.stopMu.Unlock()
	// API failures and UID conflicts do not transfer ownership of this handle's
	// local helpers. Stop those even when remote termination is unconfirmed.
	defer func() {
		result = errors.Join(result, p.stopForwards(ctx))
		if result == nil {
			p.finish(nil)
		}
	}()
	if p.uid == "" {
		return fmt.Errorf("cannot stop pod without a UID")
	}
	pod, absent, err := p.get(ctx)
	if err != nil {
		return err
	}
	if !absent && pod.Metadata.UID != p.uid {
		return fmt.Errorf("pod changed identity; refusing deletion")
	}
	if !absent {
		// Kubernetes evaluates UID preconditions atomically; a get-then-delete by
		// name would otherwise race a replacement Pod.
		body, _ := json.Marshal(map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]string{"uid": p.uid}, "gracePeriodSeconds": int64(math.Ceil(p.timeout().Seconds()))})
		_, deleteErr := p.runner.Output(ctx, body, p.binary, "delete", "--raw", "/api/v1/namespaces/"+url.PathEscape(p.namespace)+"/pods/"+url.PathEscape(p.name), "--filename", "-")
		if deleteErr != nil {
			return deleteErr
		}
		for {
			current, gone, err := p.get(ctx)
			if err != nil {
				return err
			}
			if gone {
				break
			}
			if current.Metadata.UID != p.uid {
				return fmt.Errorf("pod changed identity before termination confirmation")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
	return nil
}

func (p *kubernetesProcess) stopForwards(ctx context.Context) error {
	var failures []error
	for _, forward := range p.forwards {
		if err := forward.Stop(ctx); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

var _ Driver = (*KubernetesDriver)(nil)
var _ Process = (*kubernetesProcess)(nil)

func (d *KubernetesDriver) pending(name, id, fingerprint string) Process {
	return pending(func(ctx context.Context) error {
		pod, absent, err := d.get(ctx, name)
		if err != nil {
			return err
		}
		if absent {
			return fmt.Errorf("pod creation outcome remains unknown")
		}
		if pod.Metadata.Name != name || pod.Metadata.UID == "" || !d.owns(pod.Metadata.Labels) || pod.Metadata.Labels["workload.webong.dev/instance"] != runtimeLabelValue(id) || pod.Metadata.Labels["workload.webong.dev/fingerprint"] != fingerprint {
			return fmt.Errorf("pending pod ownership conflict")
		}
		return d.process(pod, nil).Stop(ctx)
	})
}
