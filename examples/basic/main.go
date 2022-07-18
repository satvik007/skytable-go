package main

import (
	"context"
	"fmt"

	"github.com/satvik007/skytable-go"
)

func main() {
	ctx := context.Background()

	sdb := skytable.NewClient(&skytable.Options{
		Addr: "localhost:2003",
	})

	// flush the default:default table
	if err := sdb.FlushDB(ctx, "").Err(); err != nil {
		panic(err)
	}

	// setting key: value
	if err := sdb.Set(ctx, "key", "value").Err(); err != nil {
		panic(err)
	}

	// getting key: value
	val, err := sdb.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key:", val)

	// updating key: value
	if err := sdb.Update(ctx, "key", "value2").Err(); err != nil {
		panic(err)
	}

	// getting key: value
	val, err = sdb.Get(ctx, "key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key:", val)

	// trying to overwrite key: value with set
	if err := sdb.Set(ctx, "key", "old-value").Err(); err != nil {
		fmt.Printf("%v\n", err)
	}

	// trying to update a non-existing key
	if err := sdb.Update(ctx, "key2", "value2").Err(); err != nil {
		fmt.Printf("%v\n", err)
	}
}
