//go:build linux || darwin

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// A process-held lock is sufficient for this single-host example. It is not a
// distributed lease and is never presented as fencing another machine.
func claimOwner(directory string) (*os.File, error) {
	file, err := os.OpenFile(filepath.Join(directory, "controller.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("another example controller already owns %s: %w", directory, err)
	}
	return file, nil
}
