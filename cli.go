package workload

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type commandRunner interface {
	Output(context.Context, []byte, string, ...string) (string, error)
}

type execCommandRunner struct{}

// Keep connection selection on every invocation, including cleanup and watchers.
// Never mutate process-global CLI contexts or environment variables.
type prefixedCommandRunner struct {
	runner    commandRunner
	arguments []string
}

func (r prefixedCommandRunner) Output(ctx context.Context, input []byte, binary string, arguments ...string) (string, error) {
	args := append(append([]string{}, r.arguments...), arguments...)
	return r.runner.Output(ctx, input, binary, args...)
}

func (execCommandRunner) Output(ctx context.Context, input []byte, binary string, arguments ...string) (string, error) {
	command := exec.CommandContext(ctx, binary, arguments...)
	if input != nil {
		command.Stdin = bytes.NewReader(input)
	}
	output, err := command.CombinedOutput()
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			return "", err
		}
		return "", fmt.Errorf("%w: %s", err, message)
	}

	return strings.TrimSpace(string(output)), nil
}
