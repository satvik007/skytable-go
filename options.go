package skytable

import (
	"context"
	"crypto/tls"
	"net"
	"runtime"
	"strings"
	"time"

	"github.com/satvik007/skytable-go/internal/pool"
)

// Limiter is the interface of a rate limiter or a circuit breaker.
type Limiter interface {
	// Allow returns nil if operation is allowed or an error otherwise.
	// If operation is allowed client must ReportResult of the operation
	// If operation is allowed client must ReportResult of the operation
	// whether it is a success or a failure.
	Allow() error
	// ReportResult reports the result of the previously allowed operation.
	// nil indicates a success, non-nil error usually indicates a failure.
	ReportResult(result error)
}

// Options keeps the settings to setup skytable connection.
type Options struct {
	// The network type, currently only tcp is supported.
	// Default is tcp.
	Network string
	// host:port address.
	Addr string

	// Dialer creates new network connection and has priority over
	// Network and Addr options.
	Dialer func(ctx context.Context, network, addr string) (net.Conn, error)

	// Hook that is called when new connection is established.
	OnConnect func(ctx context.Context, cn *Conn) error

	// Optional Username. Required only when authn is enabled in the configuration.
	// Use the specified Username to authenticate the current connection.
	// Authn is not enabled by default. Check the docs. https://docs.skytable.io/auth
	Username string
	// Optional Token. Required only when authn is enabled in the configuration.
	// Use the specified Token to authenticate the current connection.
	// Authn is not enabled by default. Check the docs. https://docs.skytable.io/auth
	Token string
	// CredentialsProvider allows the username and token to be updated
	// before reconnecting. It should return the current username and token.
	CredentialsProvider func() (username string, token string)

	// Table to be selected after connecting to the server.
	// FQE syntax is used to describe the full path to a table.
	// For example, if you have a keyspace supercyan and you have a table cyan within it, then the FQE syntax will be:
	//  supercyan:cyan
	//
	// !IMPORTANT NOTE
	// When you connect to Skytable, you are connected to the default keyspace which has a default table.
	Table string

	// Maximum number of retries before giving up.
	// Default is 3 retries; -1 (not 0) disables retries.
	MaxRetries int
	// Minimum backoff between each retry.
	// Default is 8 milliseconds; -1 disables backoff.
	MinRetryBackoff time.Duration
	// Maximum backoff between each retry.
	// Default is 512 milliseconds; -1 disables backoff.
	MaxRetryBackoff time.Duration

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	ReadTimeout time.Duration
	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is ReadTimeout.
	WriteTimeout time.Duration

	// Type of connection pool.
	// true for FIFO pool, false for LIFO pool.
	// Note that fifo has higher overhead compared to lifo.
	PoolFIFO bool
	// Maximum number of socket connections.
	// Default is 10 connections per every available CPU as reported by runtime.GOMAXPROCS.
	PoolSize int
	// Minimum number of idle connections which is useful when establishing
	// new connection is slow.
	MinIdleConns int
	// Connection age at which client retires (closes) the connection.
	// Default is to not close aged connections.
	MaxConnAge time.Duration
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout time.Duration
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	IdleTimeout time.Duration
	// Frequency of idle checks made by idle connections reaper.
	// Default is 1 minute. -1 disables idle connections reaper,
	// but idle connections are still discarded by the client
	// if IdleTimeout is set.
	IdleCheckFrequency time.Duration

	// TLS Config to use. When set TLS will be negotiated.
	TLSConfig *tls.Config

	// Limiter interface used to implemented circuit breaker or rate limiter.
	Limiter Limiter
}

func (opt *Options) init() {
	if opt.Addr == "" {
		opt.Addr = "localhost:2003"
	}
	if opt.Network == "" {
		opt.Network = "tcp"
	}
	if opt.Table != "" && !strings.Contains(opt.Table, ":") {
		opt.Table = "default:" + opt.Table
	}
	if opt.DialTimeout == 0 {
		opt.DialTimeout = 5 * time.Second
	}
	if opt.Dialer == nil {
		opt.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
			netDialer := &net.Dialer{
				Timeout:   opt.DialTimeout,
				KeepAlive: 5 * time.Minute,
			}
			if opt.TLSConfig == nil {
				return netDialer.DialContext(ctx, network, addr)
			}
			return tls.DialWithDialer(netDialer, network, addr, opt.TLSConfig)
		}
	}
	if opt.PoolSize == 0 {
		opt.PoolSize = 10 * runtime.GOMAXPROCS(0)
	}
	switch opt.ReadTimeout {
	case -1:
		opt.ReadTimeout = 0
	case 0:
		opt.ReadTimeout = 3 * time.Second
	}
	switch opt.WriteTimeout {
	case -1:
		opt.WriteTimeout = 0
	case 0:
		opt.WriteTimeout = opt.ReadTimeout
	}
	if opt.PoolTimeout == 0 {
		opt.PoolTimeout = opt.ReadTimeout + time.Second
	}
	if opt.IdleTimeout == 0 {
		opt.IdleTimeout = 5 * time.Minute
	}
	if opt.IdleCheckFrequency == 0 {
		opt.IdleCheckFrequency = time.Minute
	}

	if opt.MaxRetries == -1 {
		opt.MaxRetries = 0
	} else if opt.MaxRetries == 0 {
		opt.MaxRetries = 3
	}
	switch opt.MinRetryBackoff {
	case -1:
		opt.MinRetryBackoff = 0
	case 0:
		opt.MinRetryBackoff = 8 * time.Millisecond
	}
	switch opt.MaxRetryBackoff {
	case -1:
		opt.MaxRetryBackoff = 0
	case 0:
		opt.MaxRetryBackoff = 512 * time.Millisecond
	}
}

func (opt *Options) clone() *Options {
	clone := *opt
	return &clone
}

func newConnPool(opt *Options) *pool.ConnPool {
	return pool.NewConnPool(&pool.Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			return opt.Dialer(ctx, opt.Network, opt.Addr)
		},
		PoolFIFO:           opt.PoolFIFO,
		PoolSize:           opt.PoolSize,
		MinIdleConns:       opt.MinIdleConns,
		MaxConnAge:         opt.MaxConnAge,
		PoolTimeout:        opt.PoolTimeout,
		IdleTimeout:        opt.IdleTimeout,
		IdleCheckFrequency: opt.IdleCheckFrequency,
	})
}
