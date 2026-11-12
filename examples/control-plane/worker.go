package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/webong/workload"
)

// messageDefinition is where this application's knowledge of its executable
// lives. Workload chooses ports, launches the process, and manages its lifetime.
type messageDefinition struct{ executable, directory string }

func (messageDefinition) Type() string                { return "message-server" }
func (messageDefinition) Supports(driver string) bool { return driver == workload.DriverLocal }
func (messageDefinition) Validate(ctx context.Context, spec workload.Spec) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var configuration messageConfiguration
	if err := json.Unmarshal(spec.Configuration, &configuration); err != nil {
		return err
	}
	if configuration.Message == "" {
		return errors.New("message is required")
	}
	return nil
}
func (d messageDefinition) Prepare(ctx context.Context, spec workload.Spec, placement workload.Placement) (workload.LaunchSpec, error) {
	if err := d.Validate(ctx, spec); err != nil {
		return workload.LaunchSpec{}, err
	}
	var configuration messageConfiguration
	_ = json.Unmarshal(spec.Configuration, &configuration)
	port := placement.Ports["default"]
	return workload.LaunchSpec{Executable: d.executable, WorkingDirectory: d.directory, Arguments: []string{"serve", "-listen", net.JoinHostPort(placement.BindHost, strconv.Itoa(port)), "-message", configuration.Message, "-revision", strconv.FormatInt(spec.Revision, 10)}, Endpoints: []workload.EndpointSpec{{Name: "http", Port: port}}}, nil
}
func serveWorker(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(output)
	address := flags.String("listen", "127.0.0.1:0", "worker HTTP address")
	message := flags.String("message", "hello", "response message")
	revision := flags.Int64("revision", 1, "execution revision")
	if err := flags.Parse(args); err != nil {
		return err
	}
	listener, err := listenLoopback(*address)
	if err != nil {
		return err
	}
	server := &http.Server{ReadHeaderTimeout: 2 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"message": *message, "revision": *revision, "pid": os.Getpid()})
	})}
	result := make(chan error, 1)
	go func() { result <- server.Serve(listener) }()
	select {
	case err := <-result:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return fmt.Errorf("shutdown worker: %w", err)
		}
		return nil
	}
}
func listenLoopback(address string) (net.Listener, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return nil, errors.New("this unauthenticated example only listens on loopback IP addresses")
	}
	return net.Listen("tcp", address)
}
