package skytable

import (
	"context"
	"sync"
)

type pipelineExecer func(context.Context, []Cmder) error

// Pipeliner is a mechanism to realise Skytable Pipeline technique.
//
// Pipelining is a technique to extremely speed up processing by packing
// operations to batches, send them at once to Skytable and read a replies in a
// singe step.
// See https://docs.skytable.io/actions-overview/#pipelined-queries
//
// Pay attention, that Pipeline is not a transaction, so you can get unexpected
// results in case of big pipelines and small read/write timeouts.
// Skytable client has retransmission logic in case of timeouts, pipeline
// can be retransmitted and commands can be executed more then once.
// To avoid this: it is good idea to use reasonable bigger read/write timeouts
// depends of your batch size
type Pipeliner interface {
	StatefulCmdable
	Len() int
	Do(ctx context.Context, args ...interface{}) *Cmd
	Process(ctx context.Context, cmd Cmder) error
	Discard()
	Exec(ctx context.Context) ([]Cmder, error)
}

var _ Pipeliner = (*Pipeline)(nil)

// Pipeline implements pipelining as described in
// https://docs.skytable.io/protocol/skyhash#a-full-example-a-pipelined-query. It's safe for concurrent use
// by multiple goroutines.
type Pipeline struct {
	cmdable
	statefulCmdable

	exec pipelineExecer

	mu   sync.Mutex
	cmds []Cmder
}

func (c *Pipeline) init() {
	c.cmdable = c.Process
	c.statefulCmdable = c.Process
}

// Len returns the number of queued commands.
func (c *Pipeline) Len() int {
	c.mu.Lock()
	ln := len(c.cmds)
	c.mu.Unlock()
	return ln
}

// Do queues the custom command for later execution.
func (c *Pipeline) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	return cmd
}

// Process queues the cmd for later execution.
func (c *Pipeline) Process(ctx context.Context, cmd Cmder) error {
	c.mu.Lock()
	c.cmds = append(c.cmds, cmd)
	c.mu.Unlock()
	return nil
}

// Discard resets the pipeline and discards queued commands.
func (c *Pipeline) Discard() {
	c.mu.Lock()
	c.cmds = c.cmds[:0]
	c.mu.Unlock()
}

// Exec executes all previously queued commands using one
// client-server roundtrip.
//
// Exec always returns list of commands and error of the first failed
// command if any.
func (c *Pipeline) Exec(ctx context.Context) ([]Cmder, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.cmds) == 0 {
		return nil, nil
	}

	cmds := c.cmds
	c.cmds = nil

	return cmds, c.exec(ctx, cmds)
}

func (c *Pipeline) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	if err := fn(c); err != nil {
		return nil, err
	}
	return c.Exec(ctx)
}

func (c *Pipeline) Pipeline() Pipeliner {
	return c
}
