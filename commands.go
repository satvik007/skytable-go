package skytable

import (
	"context"
)

type Cmdable interface {
	Pipeline() Pipeliner
	Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error)

	CreateTable(ctx context.Context, table, keymap string, properties ...string) *StatusCmd
	CreateKeyspace(ctx context.Context, entity string) *StatusCmd
	Drop(ctx context.Context, entity string) *StatusCmd
	// Do(ctx context.Context, command string, args ...interface{}) *Cmd
	FlushDB(ctx context.Context, entity string) *StatusCmd
	Get(ctx context.Context, key string) *StringCmd
	Heya(ctx context.Context, message string) *StringCmd
	Set(ctx context.Context, key interface{}, value interface{}) *StatusCmd
	Use(ctx context.Context, entity string) *StatusCmd
}

type StatefulCmdable interface {
	Cmdable
	Login(ctx context.Context, username, token string) *StatusCmd
	Logout(ctx context.Context) *StatusCmd
}

var (
	_ Cmdable = (*Client)(nil)
)

type cmdable func(ctx context.Context, cmd Cmder) error

type statefulCmdable func(ctx context.Context, cmd Cmder) error

// ------------------------------------------------------------------------------

// func (c cmdable) Command(ctx context.Context) *CommandsInfoCmd {
// 	cmd := NewCommandsInfoCmd(ctx, "command")
// 	_ = c(ctx, cmd)
// 	return cmd
// }

// Login Attempts to log in using the provided credentials
//
// Time complexity: O(1)
//
// Operation can throw error.
// 10	Bad credentials	The authn credentials are invalid
func (c statefulCmdable) Login(ctx context.Context, username, token string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "AUTH", username, token)
	_ = c(ctx, cmd)
	return cmd
}

// Logout Attempts to log out the currently logged-in user
//
// Time complexity: O(1)
//
// Operation can throw error.
// 10	Bad credentials	The authn credentials are invalid
func (c statefulCmdable) Logout(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd(ctx, "AUTH", "LOGOUT")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CreateKeyspace(ctx context.Context, entity string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "CREATE", entity)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CreateTable(ctx context.Context, table, keymap string, properties ...string) *StatusCmd {
	args := make([]interface{}, 4, len(properties)+4)
	args[0] = "CREATE"
	args[1] = "TABLE"
	args[2] = table
	args[3] = "keymap" + keymap
	for _, prop := range properties {
		args = append(args, prop)
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Drop(ctx context.Context, entity string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "DROP", entity)
	_ = c(ctx, cmd)
	return cmd
}

// FlushDB Removes all entries stored in the current table or in the provided entity.
// Pass the entity name in FQE i.e.
//    <keyspace>:<table>
// or leave empty to flush the current table.
// Only passing the table name will flush the table in current active keyspace.
//
// Time complexity: O(n)
//
// Operation can throw error.
// 5 Server error	- An error occurred on the server side
//
// Example:
//    ctx := context.Background()
//
//    sdb := skytable.NewClient(&skytable.Options{
//      Addr: "localhost:2003",
//    })
//
//    if err := sdb.FlushDB(ctx, "").Err(); err != nil {
//      panic(err)
//    }
func (c cmdable) FlushDB(ctx context.Context, entity string) *StatusCmd {
	args := make([]interface{}, 1, 2)
	args[0] = "FLUSHDB"
	if entity != "" {
		args = append(args, entity)
	}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// Get the value of a key from the current table, if it exists
// It returns skytable.Nil error when key does not exist.
//
// Time complexity: O(1)
//
// @return the requested value
//
// The operation can throw error.
// 1	Nil	The client asked for a non-existent object
//
// Example:
//      ctx := context.Background()
//
//      sdb := skytable.NewClient(&skytable.Options{
//        Addr: "localhost:2003",
//      })
//
//      // setting key: value
//      if err := sdb.Set(ctx, "key", "value").Err(); err != nil {
//        panic(err)
//      }
//
//      // getting key: value
//      val, err := sdb.Get(ctx, "key").Result()
//      if err != nil {
//        panic(err)
//      }
//      fmt.Println("key:", val)
func (c cmdable) Get(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd(ctx, "GET", key)
	_ = c(ctx, cmd)
	return cmd
}

// Heya Either returns a "HEY!" or returns the provided argument as a str
//
// Time complexity: O(1)
func (c cmdable) Heya(ctx context.Context, message string) *StringCmd {
	var cmd *StringCmd
	if message == "" {
		cmd = NewStringCmd(ctx, "HEYA")
	} else {
		cmd = NewStringCmd(ctx, "HEYA", message)
	}
	_ = c(ctx, cmd)
	return cmd
}

// Set the value of a key in the current table, if it doesn't already exist
// Throws overwriting error if the key already exists.
//
// Time complexity: O(1)
//
// The operation can throw error.
// 2	Overwrite error	The client tried to overwrite data
// 5	Server error	  An error occurred on the server side
//
// Example:
//    ctx := context.Background()
//
//    sdb := skytable.NewClient(&skytable.Options{
//      Addr: "localhost:2003",
//    })
//
//    // setting key: value
//    if err := sdb.Set(ctx, "key", "value").Err(); err != nil {
//      panic(err)
//    }
func (c cmdable) Set(ctx context.Context, key interface{}, value interface{}) *StatusCmd {
	args := make([]interface{}, 3)
	args[0] = "SET"
	args[1] = key
	args[2] = value

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// Update the value of an existing key in the current table
//
// Time complexity: O(1)
//
// The operation can throw error.
// 1	Nil	The client asked for a non-existent object
// 5	Server error	An error occurred on the server side
//
// Example:
//    ctx := context.Background()
//
//    sdb := skytable.NewClient(&skytable.Options{
//      Addr: "localhost:2003",
//    })
//
//    // setting key: value
//    if err := sdb.Set(ctx, "key", "value").Err(); err != nil {
//      panic(err)
//    }
//
//    // updating key: value
//    if err := sdb.Update(ctx, "key", "value2").Err(); err != nil {
//      panic(err)
//    }
func (c cmdable) Update(ctx context.Context, key interface{}, value interface{}) *StatusCmd {
	args := make([]interface{}, 3)
	args[0] = "UPDATE"
	args[1] = key
	args[2] = value

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) Use(ctx context.Context, entity string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "USE", entity)
	_ = c(ctx, cmd)
	return cmd
}
