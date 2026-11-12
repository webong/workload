//go:build linux || darwin

package workload

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func configureProcessGroup(c *exec.Cmd)       { c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func terminateProcessGroup(c *exec.Cmd) error { return signalGroup(c, syscall.SIGTERM) }
func killProcessGroup(c *exec.Cmd) error      { return signalGroup(c, syscall.SIGKILL) }
func signalGroup(c *exec.Cmd, s syscall.Signal) error {
	if c.Process == nil {
		return nil
	}
	err := syscall.Kill(-c.Process.Pid, s)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
func lockSupervisor(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}
