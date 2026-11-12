//go:build !linux && !darwin

package workload

import (
	"fmt"
	"os"
	"os/exec"
)

func configureProcessGroup(c *exec.Cmd) {}
func terminateProcessGroup(c *exec.Cmd) error {
	return fmt.Errorf("local process groups require Linux or macOS")
}
func killProcessGroup(c *exec.Cmd) error {
	return fmt.Errorf("local process groups require Linux or macOS")
}
func lockSupervisor(f *os.File) error { return fmt.Errorf("local supervision requires Linux or macOS") }
