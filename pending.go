package workload

import (
	"context"
	"sync"
)

// pendingProcess retains ownership when creation may have succeeded but its
// response was lost. Stop resolves the resource's immutable identity before
// cleanup. Unknown absence is not confirmation that a timed-out create failed.
type pendingProcess struct {
	stop func(context.Context) error
	done chan struct{}
	once sync.Once
}

func pending(stop func(context.Context) error) *pendingProcess {
	return &pendingProcess{stop: stop, done: make(chan struct{})}
}
func (p *pendingProcess) PID() int              { return 0 }
func (p *pendingProcess) Host() string          { return "" }
func (p *pendingProcess) Port() int             { return 0 }
func (p *pendingProcess) Endpoints() []Endpoint { return nil }
func (p *pendingProcess) Done() <-chan struct{} { return p.done }
func (p *pendingProcess) Err() error            { return nil }
func (p *pendingProcess) Stop(ctx context.Context) error {
	if err := p.stop(ctx); err != nil {
		return err
	}
	p.once.Do(func() { close(p.done) })
	return nil
}
