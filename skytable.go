// Package skytable_go implements a client for Skytable.
//
// The client is licensed under the permissive Apache 2 license.
// You can obtain a copy of the license at
// http://www.apache.org/licenses/LICENSE-2.0.
//
// This client is a derivative of the go-redis client. See the
// go-redis license below for more details.
//
// Copyright (c) 2013 The github.com/go-redis/redis Authors.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
package skytable

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/satvik007/skytable-go/internal"
	"github.com/satvik007/skytable-go/internal/pool"
	"github.com/satvik007/skytable-go/internal/proto"
)

// Nil reply returned by Skytable when key does not exist.
const Nil = proto.Nil
const OverwriteError = proto.OverwriteError
const ActionError = proto.ActionError
const PacketError = proto.PacketError
const ServerError = proto.ServerError
const OtherError = proto.OtherError
const WrongTypeError = proto.WrongTypeError
const UnknownDataTypeError = proto.UnknownDataTypeError
const EncodingError = proto.EncodingError
const BadCredentials = proto.BadCredentials
const AuthnRealmError = proto.AuthnRealmError

// SetLogger set custom log
func SetLogger(logger internal.Logging) {
	internal.Logger = logger
}

// ------------------------------------------------------------------------------

type Hook interface {
	BeforeProcess(ctx context.Context, cmd Cmder) (context.Context, error)
	AfterProcess(ctx context.Context, cmd Cmder) error

	BeforeProcessPipeline(ctx context.Context, cmds []Cmder) (context.Context, error)
	AfterProcessPipeline(ctx context.Context, cmds []Cmder) error
}

type hooks struct {
	hooks []Hook
}

func (hs *hooks) lock() {
	hs.hooks = hs.hooks[:len(hs.hooks):len(hs.hooks)]
}

func (hs *hooks) AddHook(hook Hook) {
	hs.hooks = append(hs.hooks, hook)
}

func (hs hooks) process(
	ctx context.Context, cmd Cmder, fn func(context.Context, Cmder) error,
) error {
	if len(hs.hooks) == 0 {
		err := fn(ctx, cmd)
		cmd.SetErr(err)
		return err
	}

	var hookIndex int
	var retErr error

	for ; hookIndex < len(hs.hooks) && retErr == nil; hookIndex++ {
		ctx, retErr = hs.hooks[hookIndex].BeforeProcess(ctx, cmd)
		if retErr != nil {
			cmd.SetErr(retErr)
		}
	}

	if retErr == nil {
		retErr = fn(ctx, cmd)
		cmd.SetErr(retErr)
	}

	for hookIndex--; hookIndex >= 0; hookIndex-- {
		if err := hs.hooks[hookIndex].AfterProcess(ctx, cmd); err != nil {
			retErr = err
			cmd.SetErr(retErr)
		}
	}

	return retErr
}

func (hs hooks) processPipeline(
	ctx context.Context, cmds []Cmder, fn func(context.Context, []Cmder) error,
) error {
	if len(hs.hooks) == 0 {
		err := fn(ctx, cmds)
		return err
	}

	var hookIndex int
	var retErr error

	for ; hookIndex < len(hs.hooks) && retErr == nil; hookIndex++ {
		ctx, retErr = hs.hooks[hookIndex].BeforeProcessPipeline(ctx, cmds)
		if retErr != nil {
			setCmdsErr(cmds, retErr)
		}
	}

	if retErr == nil {
		retErr = fn(ctx, cmds)
	}

	for hookIndex--; hookIndex >= 0; hookIndex-- {
		if err := hs.hooks[hookIndex].AfterProcessPipeline(ctx, cmds); err != nil {
			retErr = err
			setCmdsErr(cmds, retErr)
		}
	}

	return retErr
}

// ------------------------------------------------------------------------------

type baseClient struct {
	opt      *Options
	connPool pool.Pooler

	onClose func() error // hook called when client is closed
}

func newBaseClient(opt *Options, connPool pool.Pooler) *baseClient {
	return &baseClient{
		opt:      opt,
		connPool: connPool,
	}
}

func (c *baseClient) clone() *baseClient {
	clone := *c
	return &clone
}

func (c *baseClient) withTimeout(timeout time.Duration) *baseClient {
	opt := c.opt.clone()
	opt.ReadTimeout = timeout
	opt.WriteTimeout = timeout

	clone := c.clone()
	clone.opt = opt

	return clone
}

func (c *baseClient) String() string {
	return fmt.Sprintf("Skytable<%s table:%s>", c.getAddr(), c.opt.Table)
}

func (c *baseClient) getConn(ctx context.Context) (*pool.Conn, error) {
	if c.opt.Limiter != nil {
		err := c.opt.Limiter.Allow()
		if err != nil {
			return nil, err
		}
	}

	cn, err := c._getConn(ctx)
	if err != nil {
		if c.opt.Limiter != nil {
			c.opt.Limiter.ReportResult(err)
		}
		return nil, err
	}

	return cn, nil
}

func (c *baseClient) _getConn(ctx context.Context) (*pool.Conn, error) {
	cn, err := c.connPool.Get(ctx)
	if err != nil {
		return nil, err
	}

	if cn.Inited {
		return cn, nil
	}

	if err := c.initConn(ctx, cn); err != nil {
		c.connPool.Remove(ctx, cn, err)
		if err := errors.Unwrap(err); err != nil {
			return nil, err
		}
		return nil, err
	}

	return cn, nil
}

func (c *baseClient) initConn(ctx context.Context, cn *pool.Conn) error {
	if cn.Inited {
		return nil
	}
	cn.Inited = true

	username, token := c.opt.Username, c.opt.Token
	if c.opt.CredentialsProvider != nil {
		username, token = c.opt.CredentialsProvider()
	}

	connPool := pool.NewSingleConnPool(c.connPool, cn)
	conn := newConn(c.opt, connPool)

	if username != "" && token != "" {
		if err := conn.Login(ctx, username, token).Err(); err != nil {
			return err
		}
	}

	if c.opt.Table != "" {
		if err := conn.Use(ctx, c.opt.Table).Err(); err != nil {
			return err
		}
	}

	if c.opt.OnConnect != nil {
		return c.opt.OnConnect(ctx, conn)
	}

	return nil
}

func (c *baseClient) releaseConn(ctx context.Context, cn *pool.Conn, err error) {
	if c.opt.Limiter != nil {
		c.opt.Limiter.ReportResult(err)
	}

	if isBadConn(err, false, c.opt.Addr) {
		c.connPool.Remove(ctx, cn, err)
	} else {
		c.connPool.Put(ctx, cn)
	}
}

func (c *baseClient) withConn(
	ctx context.Context, fn func(context.Context, *pool.Conn) error,
) error {
	cn, err := c.getConn(ctx)
	if err != nil {
		return err
	}

	defer func() {
		c.releaseConn(ctx, cn, err)
	}()

	done := ctx.Done() //nolint:ifshort

	if done == nil {
		err = fn(ctx, cn)
		return err
	}

	errc := make(chan error, 1)
	go func() { errc <- fn(ctx, cn) }()

	select {
	case <-done:
		_ = cn.Close()
		// Wait for the goroutine to finish and send something.
		<-errc

		err = ctx.Err()
		return err
	case err = <-errc:
		return err
	}
}

func (c *baseClient) process(ctx context.Context, cmd Cmder) error {
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRetries; attempt++ {
		attempt := attempt

		retry, err := c._process(ctx, cmd, attempt)
		if err == nil || !retry {
			return err
		}

		lastErr = err
	}
	return lastErr
}

func (c *baseClient) _process(ctx context.Context, cmd Cmder, attempt int) (bool, error) {
	if attempt > 0 {
		if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
			return false, err
		}
	}

	retryTimeout := uint32(1)
	err := c.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
		err := cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
			if err := wr.WriteMetaFrame(1); err != nil {
				return err
			}
			return writeCmd(wr, cmd)
		})
		if err != nil {
			return err
		}

		err = cn.WithReader(ctx, c.cmdTimeout(cmd), func(rd *proto.Reader) error {
			cnt, err := rd.ReadMetaFrame()
			if err != nil {
				return err
			}
			if cnt != 1 {
				return fmt.Errorf("skytable: expected %d commands, got %d", 1, cnt)
			}
			return cmd.readReply(rd)
		})
		if err != nil {
			if cmd.readTimeout() == nil {
				atomic.StoreUint32(&retryTimeout, 1)
			}
			return err
		}

		return nil
	})
	if err == nil {
		return false, nil
	}

	retry := shouldRetry(err, atomic.LoadUint32(&retryTimeout) == 1)
	return retry, err
}

func (c *baseClient) retryBackoff(attempt int) time.Duration {
	return internal.RetryBackoff(attempt, c.opt.MinRetryBackoff, c.opt.MaxRetryBackoff)
}

func (c *baseClient) cmdTimeout(cmd Cmder) time.Duration {
	if timeout := cmd.readTimeout(); timeout != nil {
		t := *timeout
		if t == 0 {
			return 0
		}
		return t + 10*time.Second
	}
	return c.opt.ReadTimeout
}

// Close closes the client, releasing any open resources.
//
// It is rare to Close a Client, as the Client is meant to be
// long-lived and shared between many goroutines.
func (c *baseClient) Close() error {
	var firstErr error
	if c.onClose != nil {
		if err := c.onClose(); err != nil {
			firstErr = err
		}
	}
	if err := c.connPool.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (c *baseClient) getAddr() string {
	return c.opt.Addr
}

func (c *baseClient) processPipeline(ctx context.Context, cmds []Cmder) error {
	return c.generalProcessPipeline(ctx, cmds, c.pipelineProcessCmds)
}

type pipelineProcessor func(context.Context, *pool.Conn, []Cmder) (bool, error)

func (c *baseClient) generalProcessPipeline(
	ctx context.Context, cmds []Cmder, p pipelineProcessor,
) error {
	err := c._generalProcessPipeline(ctx, cmds, p)
	if err != nil {
		setCmdsErr(cmds, err)
		return err
	}
	return cmdsFirstErr(cmds)
}

func (c *baseClient) _generalProcessPipeline(
	ctx context.Context, cmds []Cmder, p pipelineProcessor,
) error {
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRetries; attempt++ {
		if attempt > 0 {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				return err
			}
		}

		var canRetry bool
		lastErr = c.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
			var err error
			canRetry, err = p(ctx, cn, cmds)
			return err
		})
		if lastErr == nil || !canRetry || !shouldRetry(lastErr, true) {
			return lastErr
		}
	}
	return lastErr
}

func (c *baseClient) pipelineProcessCmds(
	ctx context.Context, cn *pool.Conn, cmds []Cmder,
) (bool, error) {
	err := cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
		return writeCmds(wr, cmds)
	})
	if err != nil {
		return true, err
	}

	err = cn.WithReader(ctx, c.opt.ReadTimeout, func(rd *proto.Reader) error {
		return pipelineReadCmds(rd, cmds)
	})
	return true, err
}

func pipelineReadCmds(rd *proto.Reader, cmds []Cmder) error {
	cnt, err := rd.ReadMetaFrame()
	if err != nil {
		return err
	}
	if cnt != len(cmds) {
		return fmt.Errorf("skytable: expected %d commands, got %d", len(cmds), cnt)
	}
	for _, cmd := range cmds {
		err := cmd.readReply(rd)
		cmd.SetErr(err)
		if err != nil && !isSkytableError(err) {
			return err
		}
	}
	return nil
}

// ------------------------------------------------------------------------------

// Client is a Skytable client representing a pool of zero or more underlying connections.
// It's safe for concurrent use by multiple goroutines.
//
// Client creates and frees connections automatically; it also maintains a free pool
// of idle connections. You can control the pool size with Config.PoolSize option.
type Client struct {
	*baseClient
	cmdable
	hooks
	ctx context.Context
}

// NewClient returns a client to the Skytable Server specified by Options.
func NewClient(opt *Options) *Client {
	opt.init()

	c := Client{
		baseClient: newBaseClient(opt, newConnPool(opt)),
		ctx:        context.Background(),
	}
	c.cmdable = c.Process

	return &c
}

func (c *Client) clone() *Client {
	clone := *c
	clone.cmdable = clone.Process
	clone.hooks.lock()
	return &clone
}

func (c *Client) WithTimeout(timeout time.Duration) *Client {
	clone := c.clone()
	clone.baseClient = c.baseClient.withTimeout(timeout)
	return clone
}

func (c *Client) Conn() *Conn {
	return newConn(c.opt, pool.NewStickyConnPool(c.connPool))
}

// Do creates a Cmd from the args and processes the cmd.
func (c *Client) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	return cmd
}

func (c *Client) Process(ctx context.Context, cmd Cmder) error {
	return c.hooks.process(ctx, cmd, c.baseClient.process)
}

func (c *Client) processPipeline(ctx context.Context, cmds []Cmder) error {
	return c.hooks.processPipeline(ctx, cmds, c.baseClient.processPipeline)
}

// Options returns read-only Options that were used to create the client.
func (c *Client) Options() *Options {
	return c.opt
}

type PoolStats pool.Stats

// PoolStats returns connection pool stats.
func (c *Client) PoolStats() *PoolStats {
	stats := c.connPool.Stats()
	return (*PoolStats)(stats)
}

func (c *Client) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

func (c *Client) Pipeline() Pipeliner {
	pipe := Pipeline{
		exec: c.processPipeline,
	}
	pipe.init()
	return &pipe
}

// ------------------------------------------------------------------------------

type conn struct {
	baseClient
	cmdable
	statefulCmdable
	hooks // TODO: inherit hooks
}

// Conn represents a single Skytable connection rather than a pool of connections.
// Prefer running commands from Client unless there is a specific need
// for a continuous single Skytable connection.
type Conn struct {
	*conn
}

func newConn(opt *Options, connPool pool.Pooler) *Conn {
	c := Conn{
		conn: &conn{
			baseClient: baseClient{
				opt:      opt,
				connPool: connPool,
			},
		},
	}
	c.cmdable = c.Process
	c.statefulCmdable = c.Process
	return &c
}

func (c *Conn) Process(ctx context.Context, cmd Cmder) error {
	return c.hooks.process(ctx, cmd, c.baseClient.process)
}

func (c *Conn) processPipeline(ctx context.Context, cmds []Cmder) error {
	return c.hooks.processPipeline(ctx, cmds, c.baseClient.processPipeline)
}

func (c *Conn) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

func (c *Conn) Pipeline() Pipeliner {
	pipe := Pipeline{
		exec: c.processPipeline,
	}
	pipe.init()
	return &pipe
}
