package workload

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type localRequest struct {
	Action, Fingerprint  string
	Launch               LaunchSpec
	NodeID, AssignmentID string
	Deadline             time.Time
}
type localResponse struct {
	Started, Done    bool
	PID              int
	Fingerprint      string
	Launch           LaunchSpec
	ExitError, Error string
}
type localRecord struct {
	NodeID, AssignmentID, Socket, Fingerprint string
	Done                                      bool
}

func readLocalRecord(path string) (localRecord, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return localRecord{}, err
	}
	var r localRecord
	err = json.Unmarshal(b, &r)
	return r, err
}
func localCall(ctx context.Context, socket string, request localRequest) (localResponse, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return localResponse{}, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		request.Deadline = deadline
		_ = conn.SetDeadline(deadline)
	}
	if err = json.NewEncoder(conn).Encode(request); err != nil {
		return localResponse{}, err
	}
	var response localResponse
	if err = json.NewDecoder(conn).Decode(&response); err != nil {
		return response, err
	}
	if response.Error != "" {
		return response, fmt.Errorf("local supervisor: %s", response.Error)
	}
	return response, nil
}

// ServeLocalSupervisor is the companion executable's entry point. The lock is
// held for the entire child lifetime, independently of any worker process. The
// private socket is the only way to adopt or stop a child; PID files are never
// trusted for signaling. A supervisor crash leaves its journal behind so a new
// supervisor fails closed instead of creating a potentially duplicate workload.
func ServeLocalSupervisor(dir, key string, stopTimeout time.Duration) error {
	if _, err := hex.DecodeString(key); err != nil {
		return err
	}
	if len(key) != 64 || stopTimeout <= 0 {
		return fmt.Errorf("invalid supervisor key or timeout")
	}
	if err := privateDirectory(dir); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(dir, key+".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = lockSupervisor(lock); err != nil {
		return fmt.Errorf("assignment already supervised: %w", err)
	}
	recordPath := filepath.Join(dir, key+".json")
	if _, err = os.Stat(recordPath); err == nil {
		record, readErr := readLocalRecord(recordPath)
		if readErr != nil || !record.Done {
			return fmt.Errorf("orphaned local journal; termination must be verified before removing %s", recordPath)
		}
		if err = os.Remove(recordPath); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	socket, err := localSocketPath(dir, key)
	if err != nil {
		return err
	}
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(socket)
	if err = os.Chmod(socket, 0600); err != nil {
		return err
	}
	var child *localProcess
	var fingerprint string
	var launch LaunchSpec
	var mu sync.Mutex
	var handlers sync.WaitGroup
	idle := time.AfterFunc(30*time.Second, func() {
		mu.Lock()
		defer mu.Unlock()
		if child == nil {
			_ = listener.Close()
		}
	})
	defer idle.Stop()
	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		handlers.Add(1)
		go func() {
			defer handlers.Done()
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(stopTimeout + time.Second))
			var request localRequest
			if err := json.NewDecoder(conn).Decode(&request); err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			response := localResponse{Fingerprint: fingerprint, Launch: launch}
			shutdown := false
			switch request.Action {
			case "start":
				if !request.Deadline.IsZero() && !time.Now().Before(request.Deadline) {
					response.Error = "start deadline expired"
					break
				}
				if child != nil {
					if fingerprint != request.Fingerprint {
						response.Error = "launch fingerprint conflict"
					}
				} else {
					if err := request.Launch.validate(false); err != nil {
						response.Error = err.Error()
						break
					}
					record := localRecord{NodeID: request.NodeID, AssignmentID: request.AssignmentID, Socket: socket, Fingerprint: request.Fingerprint}
					data, _ := json.Marshal(record)
					// Journal first: a crash in the fork/exec window must block relaunch.
					f, err := os.OpenFile(recordPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
					if err == nil {
						_, err = f.Write(data)
						if err == nil {
							err = f.Sync()
						}
						closeErr := f.Close()
						if err == nil {
							err = closeErr
						}
					}
					if err != nil {
						response.Error = err.Error()
						break
					}
					c := exec.Command(request.Launch.Executable, request.Launch.Arguments...)
					c.Dir = request.Launch.WorkingDirectory
					c.Env = append(os.Environ(), request.Launch.Environment...)
					configureProcessGroup(c)
					if err = c.Start(); err != nil {
						_ = os.Remove(recordPath)
						response.Error = err.Error()
						break
					}
					child = &localProcess{command: c, done: make(chan struct{})}
					launch = request.Launch
					fingerprint = request.Fingerprint
					go child.wait()
				}
			case "status":
			case "stop":
				if request.Fingerprint != "" && request.Fingerprint != fingerprint {
					response.Error = "launch fingerprint conflict"
					break
				}
				if child != nil {
					ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
					if !request.Deadline.IsZero() {
						var deadlineCancel context.CancelFunc
						ctx, deadlineCancel = context.WithDeadline(ctx, request.Deadline)
						defer deadlineCancel()
					}
					// Reserve half the budget to kill and reap a process that ignores TERM.
					deadline, _ := ctx.Deadline()
					grace, graceCancel := context.WithDeadline(ctx, time.Now().Add(time.Until(deadline)/2))
					_ = child.Stop(grace)
					graceCancel()
					select {
					case <-child.Done():
						if err := child.groupError(); err != nil {
							response.Error = err.Error()
						}
					case <-ctx.Done():
						response.Error = "process termination unconfirmed"
					}
					cancel()
				}
				if response.Error == "" {
					record, err := readLocalRecord(recordPath)
					if os.IsNotExist(err) && child == nil {
						shutdown = true
					} else if err != nil {
						response.Error = err.Error()
					} else {
						record.Done = true
						if err = writeLocalRecord(recordPath, record); err != nil {
							response.Error = err.Error()
						} else {
							shutdown = true
						}
					}
				}
			default:
				response.Error = "unknown supervisor action"
			}
			response.Started = child != nil
			response.Fingerprint = fingerprint
			response.Launch = launch
			if child != nil {
				response.PID = child.PID()
				select {
				case <-child.Done():
					response.Done = true
					if err := child.Err(); err != nil {
						response.ExitError = err.Error()
					}
				default:
				}
			}
			if shutdown {
				response.Done = true
			}
			_ = json.NewEncoder(conn).Encode(response)
			if shutdown {
				_ = listener.Close()
			}
		}()
	}
	handlers.Wait()
	return nil
}

func writeLocalRecord(path string, record localRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".record-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
