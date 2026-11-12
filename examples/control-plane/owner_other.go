//go:build !linux && !darwin

package main

import (
	"fmt"
	"os"
)

func claimOwner(string) (*os.File, error) {
	return nil, fmt.Errorf("the local-process example requires Linux or macOS")
}
