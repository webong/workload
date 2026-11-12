// Command control-plane demonstrates a Go app with its own in-memory control
// plane and real local workloads. See README.md in this directory for a walkthrough.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/webong/workload"
)

type appConfig struct {
	listen, stateDirectory, supervisor string
	lease, poll                        time.Duration
}

func main() {
	if err := command(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func command(args []string, output io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(args) > 0 && args[0] == "serve" {
		return serveWorker(ctx, args[1:], output)
	}
	flags := flag.NewFlagSet("control-plane", flag.ContinueOnError)
	flags.SetOutput(output)
	config := appConfig{lease: 10 * time.Second, poll: 250 * time.Millisecond}
	flags.StringVar(&config.listen, "listen", "127.0.0.1:8080", "loopback control-plane API address")
	flags.StringVar(&config.stateDirectory, "state-dir", ".workload-example", "private local supervisor state directory")
	flags.StringVar(&config.supervisor, "supervisor", "workload-supervisor", "path to the companion supervisor")
	flags.DurationVar(&config.lease, "lease-duration", 10*time.Second, "assignment lifetime renewed by successful control-plane polls")
	if err := flags.Parse(args); err != nil {
		return err
	}
	return run(ctx, config, output)
}
func run(ctx context.Context, config appConfig, output io.Writer) error {
	if config.lease < 2*time.Second {
		return errors.New("lease-duration must be at least two seconds")
	}
	stateDir, err := filepath.Abs(config.stateDirectory)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(stateDir, 0700); err != nil {
		return err
	}
	owner, err := claimOwner(stateDir)
	if err != nil {
		return err
	}
	defer owner.Close()
	supervisor, err := exec.LookPath(config.supervisor)
	if err != nil {
		return fmt.Errorf("find supervisor (build ./cmd/workload-supervisor first): %w", err)
	}
	supervisor, err = filepath.Abs(supervisor)
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	driver, err := workload.NewLocalDriver(workload.LocalDriverConfig{NodeID: "demo-node", StateDirectory: stateDir, SupervisorBinary: supervisor, StartTimeout: 5 * time.Second, StopTimeout: 5 * time.Second})
	if err != nil {
		return err
	}
	definition := messageDefinition{executable: executable, directory: stateDir}
	registry, err := workload.NewDriverRegistry(map[string]workload.Driver{workload.DriverLocal: driver}, []workload.Definition{definition})
	if err != nil {
		return err
	}
	plane := newControlPlane("demo-node", config.lease)
	reconciler, err := workload.NewReconciler(workload.ReconcilerConfig{NodeID: "demo-node", Drivers: []string{workload.DriverLocal}, PollInterval: config.poll, CoordinationLease: config.lease, StartTimeout: 5 * time.Second, StopTimeout: 5 * time.Second}, plane, registry, workload.DiscardEndpoints{}, appLogger{log.New(output, "", log.LstdFlags)})
	if err != nil {
		return err
	}
	listener, err := listenLoopback(config.listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: plane.handler(), ReadHeaderTimeout: 2 * time.Second}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	runtimeCtx, cancelRuntime := context.WithCancel(context.Background())
	defer cancelRuntime()
	runtimeDone := make(chan error, 1)
	go func() { runtimeDone <- reconciler.Run(runtimeCtx) }()
	fmt.Fprintf(output, "control plane: http://%s\nstate directory: %s\n", listener.Addr(), stateDir)
	var runtimeErr, serverErr error
	runtimeExited := false
	serverExited := false
	select {
	case <-ctx.Done():
	case runtimeErr = <-runtimeDone:
		runtimeExited = true
	case serverErr = <-serverDone:
		serverExited = true
	}
	if errors.Is(serverErr, http.ErrServerClosed) {
		serverErr = nil
	}
	// Stop admission before releasing the runtime's ownership of any processes.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err = server.Shutdown(shutdownCtx); err != nil {
		serverErr = errors.Join(serverErr, err)
		_ = server.Close()
	}
	cancel()
	if !serverExited {
		if serveErr := <-serverDone; !errors.Is(serveErr, http.ErrServerClosed) {
			serverErr = errors.Join(serverErr, serveErr)
		}
	}
	cancelRuntime()
	if !runtimeExited {
		runtimeErr = <-runtimeDone
	}
	if runtimeErr != nil {
		return fmt.Errorf("workload shutdown unresolved; retain state directory %s for recovery: %w", stateDir, errors.Join(runtimeErr, serverErr))
	}
	return serverErr
}

type appLogger struct{ *log.Logger }

func (appLogger) Debug(string, ...interface{})               {}
func (l appLogger) Info(format string, args ...interface{})  { l.Printf("INFO "+format, args...) }
func (l appLogger) Warn(format string, args ...interface{})  { l.Printf("WARN "+format, args...) }
func (l appLogger) Error(format string, args ...interface{}) { l.Printf("ERROR "+format, args...) }
