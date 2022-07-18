package skytable_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/satvik007/skytable-go"
)

type skytableHookError struct {
	skytable.Hook
}

var _ skytable.Hook = skytableHookError{}

func (skytableHookError) BeforeProcess(ctx context.Context, cmd skytable.Cmder) (context.Context, error) {
	return ctx, nil
}

func (skytableHookError) AfterProcess(ctx context.Context, cmd skytable.Cmder) error {
	return errors.New("hook error")
}

func TestHookError(t *testing.T) {
	rdb := skytable.NewClient(&skytable.Options{
		Addr: "localhost:2003",
	})
	rdb.AddHook(skytableHookError{})

	err := rdb.Heya(ctx, "").Err()
	if err == nil {
		t.Fatalf("got nil, expected an error")
	}

	wanted := "hook error"
	if err.Error() != wanted {
		t.Fatalf(`got %q, wanted %q`, err, wanted)
	}
}

// ------------------------------------------------------------------------------

var _ = Describe("Client", func() {
	var client *skytable.Client

	BeforeEach(func() {
		client = skytable.NewClient(skytableOptions())
		Expect(client.FlushDB(ctx, "").Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		client.Close()
	})

	It("should Stringer", func() {
		Expect(client.String()).To(Equal("Skytable<localhost:2003 table:default:test15>"))
	})

	It("supports context", func() {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		err := client.Heya(ctx, "").Err()
		Expect(err).To(MatchError("context canceled"))
	})

	It("should ping", func() {
		val, err := client.Heya(ctx, "").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("HEY!"))
	})

	It("should return pool stats", func() {
		Expect(client.PoolStats()).To(BeAssignableToTypeOf(&skytable.PoolStats{}))
	})

	It("should support custom dialers", func() {
		custom := skytable.NewClient(&skytable.Options{
			Network: "tcp",
			Addr:    skytableAddr,
			Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		})

		val, err := custom.Heya(ctx, "").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("HEY!"))
		Expect(custom.Close()).NotTo(HaveOccurred())
	})

	It("should close", func() {
		Expect(client.Close()).NotTo(HaveOccurred())
		err := client.Heya(ctx, "").Err()
		Expect(err).To(MatchError("skytable: client is closed"))
	})

	It("should select DB", func() {
		db2 := skytable.NewClient(&skytable.Options{
			Addr:  skytableAddr,
			Table: "test2",
		})
		Expect(db2.FlushDB(ctx, "").Err()).NotTo(HaveOccurred())
		Expect(db2.Get(ctx, "db").Err()).To(Equal(skytable.Nil))
		Expect(db2.Set(ctx, "db", 2).Err()).NotTo(HaveOccurred())

		n, err := db2.Get(ctx, "db").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(2)))

		Expect(client.Get(ctx, "db").Err()).To(Equal(skytable.Nil))

		Expect(db2.FlushDB(ctx, "").Err()).NotTo(HaveOccurred())
		Expect(db2.Close()).NotTo(HaveOccurred())
	})

	It("processes custom commands", func() {
		cmd := skytable.NewCmd(ctx, "HEYA")
		_ = client.Process(ctx, cmd)

		// Flush buffers.
		Expect(client.Heya(ctx, "hello").Err()).NotTo(HaveOccurred())

		Expect(cmd.Err()).NotTo(HaveOccurred())
		Expect(cmd.Val()).To(Equal("HEY!"))
	})

	It("should retry command on network error", func() {
		Expect(client.Close()).NotTo(HaveOccurred())

		client = skytable.NewClient(&skytable.Options{
			Addr:       skytableAddr,
			MaxRetries: 1,
		})

		// Put bad connection in the pool.
		cn, err := client.Pool().Get(ctx)
		Expect(err).NotTo(HaveOccurred())

		cn.SetNetConn(&badConn{})
		client.Pool().Put(ctx, cn)

		err = client.Heya(ctx, "").Err()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should retry with backoff", func() {
		clientNoRetry := skytable.NewClient(&skytable.Options{
			Addr:       ":1234",
			MaxRetries: -1,
		})
		defer clientNoRetry.Close()

		clientRetry := skytable.NewClient(&skytable.Options{
			Addr:            ":1234",
			MaxRetries:      5,
			MaxRetryBackoff: 128 * time.Millisecond,
		})
		defer clientRetry.Close()

		startNoRetry := time.Now()
		err := clientNoRetry.Heya(ctx, "").Err()
		Expect(err).To(HaveOccurred())
		elapseNoRetry := time.Since(startNoRetry)

		startRetry := time.Now()
		err = clientRetry.Heya(ctx, "").Err()
		Expect(err).To(HaveOccurred())
		elapseRetry := time.Since(startRetry)

		Expect(elapseRetry).To(BeNumerically(">", elapseNoRetry, 10*time.Millisecond))
	})

	It("should update conn.UsedAt on read/write", func() {
		cn, err := client.Pool().Get(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(cn.UsedAt).NotTo(BeZero())

		// set cn.SetUsedAt(time) or time.Sleep(>1*time.Second)
		// simulate the last time Conn was used
		// time.Sleep() is not the standard sleep time
		// link: https://go-review.googlesource.com/c/go/+/232298
		cn.SetUsedAt(time.Now().Add(-1 * time.Second))
		createdAt := cn.UsedAt()

		client.Pool().Put(ctx, cn)
		Expect(cn.UsedAt().Equal(createdAt)).To(BeTrue())

		err = client.Heya(ctx, "").Err()
		Expect(err).NotTo(HaveOccurred())

		cn, err = client.Pool().Get(context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(cn).NotTo(BeNil())
		Expect(cn.UsedAt().After(createdAt)).To(BeTrue())
	})

	It("should process command with special chars", func() {
		set := client.Set(ctx, "key", "hello1\r\nhello2\r\n")
		Expect(set.Err()).NotTo(HaveOccurred())
		Expect(set.Val()).To(Equal(int64(0)))

		get := client.Get(ctx, "key")
		Expect(get.Err()).NotTo(HaveOccurred())
		Expect(get.Val()).To(Equal("hello1\r\nhello2\r\n"))
	})

	It("should handle big vals", func() {
		bigVal := bytes.Repeat([]byte{'*'}, 2e6)

		err := client.Set(ctx, "key", bigVal).Err()
		Expect(err).NotTo(HaveOccurred())

		// Reconnect to get new connection.
		Expect(client.Close()).NotTo(HaveOccurred())
		client = skytable.NewClient(skytableOptions())

		got, err := client.Get(ctx, "key").Bytes()
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(bigVal))
	})

	It("should Conn", func() {
		err := client.Conn().Get(ctx, "this-key-does-not-exist").Err()
		Expect(err).To(Equal(skytable.Nil))
	})
})

var _ = Describe("Client timeout", func() {
	var opt *skytable.Options
	var client *skytable.Client

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	testTimeout := func() {
		It("Ping timeouts", func() {
			err := client.Heya(ctx, "").Err()
			Expect(err).To(HaveOccurred())
			Expect(err.(net.Error).Timeout()).To(BeTrue())
		})

		It("Pipeline timeouts", func() {
			_, err := client.Pipelined(ctx, func(pipe skytable.Pipeliner) error {
				pipe.Heya(ctx, "")
				return nil
			})
			Expect(err).To(HaveOccurred())
			Expect(err.(net.Error).Timeout()).To(BeTrue())
		})
	}

	Context("read timeout", func() {
		BeforeEach(func() {
			opt = skytableOptions()
			opt.ReadTimeout = time.Nanosecond
			opt.WriteTimeout = -1
			client = skytable.NewClient(opt)
		})

		testTimeout()
	})

	Context("write timeout", func() {
		BeforeEach(func() {
			opt = skytableOptions()
			opt.ReadTimeout = -1
			opt.WriteTimeout = time.Nanosecond
			client = skytable.NewClient(opt)
		})

		testTimeout()
	})
})

// var _ = Describe("Client context cancellation", func() {
// 	var opt *skytable.Options
// 	var client *skytable.Client
//
// 	BeforeEach(func() {
// 		opt = skytableOptions()
// 		opt.ReadTimeout = -1
// 		opt.WriteTimeout = -1
// 		client = skytable.NewClient(opt)
// 	})
//
// 	AfterEach(func() {
// 		Expect(client.Close()).NotTo(HaveOccurred())
// 	})
//
// 	It("Blocking operation cancelation", func() {
// 		ctx, cancel := context.WithCancel(ctx)
// 		cancel()
//
// 		err := client.BLPop(ctx, 1*time.Second, "test").Err()
// 		Expect(err).To(HaveOccurred())
// 		Expect(err).To(BeIdenticalTo(context.Canceled))
// 	})
// })
