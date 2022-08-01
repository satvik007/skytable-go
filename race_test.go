package skytable_test

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/satvik007/skytable-go"
)

var _ = Describe("races", func() {
	var client *skytable.Client
	var C, N int

	BeforeEach(func() {
		client = skytable.NewClient(skytableOptions())

		for i := 0; i <= 15; i++ {
			Expect(client.FlushDB(ctx, "test"+strconv.Itoa(i)).Err()).To(BeNil())
		}

		C, N = 10, 1000
		if testing.Short() {
			C = 4
			N = 100
		}
	})

	AfterEach(func() {
		err := client.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should echo", func() {
		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				msg := fmt.Sprintf("echo %d %d", id, i)
				echo, err := client.Heya(ctx, msg).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(echo).To(Equal(msg))
			}
		})
	})

	// It("should handle many keys", func() {
	//   perform(C, func(id int) {
	//     for i := 0; i < N; i++ {
	//       err := client.Set(
	//         ctx,
	//         fmt.Sprintf("keys.key-%d-%d", id, i),
	//         fmt.Sprintf("hello-%d-%d", id, i),
	//       ).Err()
	//       Expect(err).NotTo(HaveOccurred())
	//     }
	//   })
	//
	//   keys := client.Keys(ctx, "keys.*")
	//   Expect(keys.Err()).NotTo(HaveOccurred())
	//   Expect(len(keys.Val())).To(Equal(C * N))
	// })

	// It("should handle many keys 2", func() {
	//   perform(C, func(id int) {
	//     keys := []string{"non-existent-key"}
	//     for i := 0; i < N; i++ {
	//       key := fmt.Sprintf("keys.key-%d", i)
	//       keys = append(keys, key)
	//
	//       err := client.Set(ctx, key, fmt.Sprintf("hello-%d", i)).Err()
	//       Expect(err).NotTo(HaveOccurred())
	//     }
	//     keys = append(keys, "non-existent-key")
	//
	//     vals, err := client.MGet(ctx, keys...).Result()
	//     Expect(err).NotTo(HaveOccurred())
	//     Expect(len(vals)).To(Equal(N + 2))
	//
	//     for i := 0; i < N; i++ {
	//       Expect(vals[i+1]).To(Equal(fmt.Sprintf("hello-%d", i)))
	//     }
	//
	//     Expect(vals[0]).To(BeNil())
	//     Expect(vals[N+1]).To(BeNil())
	//   })
	// })

	It("should handle big vals in Get", func() {
		C, N = 4, 100

		bigVal := bigVal()

		err := client.Set(ctx, "key", bigVal).Err()
		Expect(err).NotTo(HaveOccurred())

		// Reconnect to get new connection.
		Expect(client.Close()).To(BeNil())
		client = skytable.NewClient(skytableOptions())

		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				got, err := client.Get(ctx, "key").Bytes()
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(bigVal))
			}
		})
	})

	It("should handle big vals in Set", func() {
		C, N = 4, 100

		bigVal := bigVal()
		perform(C, func(id int) {
			err := client.Set(ctx, "key"+strconv.Itoa(id), bigVal).Err()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	It("should select db", func() {
		err := client.Set(ctx, "db", 1).Err()
		Expect(err).NotTo(HaveOccurred())

		perform(C, func(id int) {
			opt := skytableOptions()
			opt.Table = "test" + strconv.Itoa(id)
			client := skytable.NewClient(opt)

			err := client.Set(ctx, "db", id).Err()
			Expect(err).NotTo(HaveOccurred())

			for i := 0; i < N; i++ {
				err := client.Update(ctx, "db", id).Err()
				Expect(err).NotTo(HaveOccurred())

				n, err := client.Get(ctx, "db").Int64()
				Expect(err).NotTo(HaveOccurred())
				Expect(n).To(Equal(int64(id)))
			}
			err = client.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		n, err := client.Get(ctx, "db").Int64()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(1)))
	})

	It("should select DB with read timeout", func() {
		perform(C, func(id int) {
			opt := skytableOptions()
			opt.Table = "table-" + strconv.Itoa(id)
			opt.ReadTimeout = time.Nanosecond
			client := skytable.NewClient(opt)

			perform(C, func(id int) {
				err := client.Heya(ctx, "").Err()
				Expect(err).To(HaveOccurred())
				Expect(err.(net.Error).Timeout()).To(BeTrue())
			})

			err := client.Close()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	It("should Pipeline", func() {
		perform(C, func(id int) {
			pipe := client.Pipeline()
			for i := 0; i < N; i++ {
				pipe.Heya(ctx, fmt.Sprint(i))
			}

			cmds, err := pipe.Exec(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cmds).To(HaveLen(N))

			for i := 0; i < N; i++ {
				Expect(cmds[i].(*skytable.StringCmd).Val()).To(Equal(fmt.Sprint(i)))
			}
		})
	})

	// It("should Pipeline", func() {
	//   pipe := client.Pipeline()
	//   perform(N, func(id int) {
	//     pipe.Incr(ctx, "key")
	//   })
	//
	//   cmds, err := pipe.Exec(ctx)
	//   Expect(err).NotTo(HaveOccurred())
	//   Expect(cmds).To(HaveLen(N))
	//
	//   n, err := client.Get(ctx, "key").Int64()
	//   Expect(err).NotTo(HaveOccurred())
	//   Expect(n).To(Equal(int64(N)))
	// })

	// PIt("should BLPop", func() {
	//   var received uint32
	//
	//   wg := performAsync(C, func(id int) {
	//     for {
	//       v, err := client.BLPop(ctx, 5*time.Second, "list").Result()
	//       if err != nil {
	//         if err == skytable.Nil {
	//           break
	//         }
	//         Expect(err).NotTo(HaveOccurred())
	//       }
	//       Expect(v).To(Equal([]string{"list", "hello"}))
	//       atomic.AddUint32(&received, 1)
	//     }
	//   })
	//
	//   perform(C, func(id int) {
	//     for i := 0; i < N; i++ {
	//       err := client.LPush(ctx, "list", "hello").Err()
	//       Expect(err).NotTo(HaveOccurred())
	//     }
	//   })
	//
	//   wg.Wait()
	//   Expect(atomic.LoadUint32(&received)).To(Equal(uint32(C * N)))
	// })
})

func bigVal() []byte {
	return bytes.Repeat([]byte{'*'}, 1<<17) // 128kb
}
