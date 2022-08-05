package skytable_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"

	"github.com/satvik007/skytable-go"
)

var (
	ctx = context.Background()
	rdb *skytable.Client
)

var _ = Describe("example_test", func() {

	rdb = skytable.NewClient(&skytable.Options{
		Addr:         "localhost:2003",
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	})

	BeforeEach(func() {
		rdb.FlushDB(ctx, "")
	})

	It("ExampleNewClient", func() {
		rdb := skytable.NewClient(&skytable.Options{
			Addr: "localhost:2003", // use default Addr
		})

		pong, err := rdb.Heya(ctx, "").Result()
		fmt.Println(pong, err)
		// Output: HEY! <nil>
	})

	It("ExampleClient", func() {
		err := rdb.Set(ctx, "key", "value").Err()
		if err != nil {
			panic(err)
		}

		val, err := rdb.Get(ctx, "key").Result()
		if err != nil {
			panic(err)
		}
		fmt.Println("key", val)

		val2, err := rdb.Get(ctx, "missing_key").Result()
		if err == skytable.Nil {
			fmt.Println("missing_key does not exist")
		} else if err != nil {
			panic(err)
		} else {
			fmt.Println("missing_key", val2)
		}
		// Output: key value
		// missing_key does not exist
	})

	It("ExampleClient_Set", func() {
		// Last argument is expiration. Zero means the key has no
		// expiration time.
		err := rdb.Set(ctx, "key", "value").Err()
		if err != nil {
			panic(err)
		}

		// key2 will expire in an hour.
		err = rdb.Set(ctx, "key2", "value").Err()
		if err != nil {
			panic(err)
		}
	})
})

// func ExampleConn() {
//   conn := rdb.Conn()
//
//   err := conn.ClientSetName(ctx, "foobar").Err()
//   if err != nil {
//     panic(err)
//   }
//
//   // Open other connections.
//   for i := 0; i < 10; i++ {
//     go rdb.Ping(ctx)
//   }
//
//   s, err := conn.ClientGetName(ctx).Result()
//   if err != nil {
//     panic(err)
//   }
//   fmt.Println(s)
//   // Output: foobar
// }

// func ExampleClient_SetEx() {
//   err := rdb.SetEx(ctx, "key", "value").Err()
//   if err != nil {
//     panic(err)
//   }
// }

// func ExampleClient_Incr() {
//   result, err := rdb.Incr(ctx, "counter").Result()
//   if err != nil {
//     panic(err)
//   }
//
//   fmt.Println(result)
//   // Output: 1
// }

// func ExampleClient_Pipelined() {
//   var incr *skytable.IntCmd
//   _, err := rdb.Pipelined(ctx, func(pipe skytable.Pipeliner) error {
//     incr = pipe.Incr(ctx, "pipelined_counter")
//     pipe.Expire(ctx, "pipelined_counter", time.Hour)
//     return nil
//   })
//   fmt.Println(incr.Val(), err)
//   // Output: 1 <nil>
// }

// func ExampleClient_Pipeline() {
//   pipe := rdb.Pipeline()
//
//   incr := pipe.Incr(ctx, "pipeline_counter")
//   pipe.Expire(ctx, "pipeline_counter", time.Hour)
//
//   // Execute
//   //
//   //     INCR pipeline_counter
//   //     EXPIRE pipeline_counts 3600
//   //
//   // using one rdb-server roundtrip.
//   _, err := pipe.Exec(ctx)
//   fmt.Println(incr.Val(), err)
//   // Output: 1 <nil>
// }

// func Example_customCommand() {
//   Get := func(ctx context.Context, rdb *skytable.Client, key string) *skytable.StringCmd {
//     cmd := skytable.NewStringCmd(ctx, "get", key)
//     rdb.Process(ctx, cmd)
//     return cmd
//   }
//
//   v, err := Get(ctx, rdb, "key_does_not_exist").Result()
//   fmt.Printf("%q %s", v, err)
//   // Output: "" skytable: nil
// }

// func Example_customCommand2() {
//   v, err := rdb.Do(ctx, "get", "key_does_not_exist").Text()
//   fmt.Printf("%q %s", v, err)
//   // Output: "" skytable: nil
// }
