package skytable

import (
	"context"
	"strings"
)

type Cmdable interface {
	Pipeline() Pipeliner
	Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error)

	AddUser(ctx context.Context, username string) *StringCmd
	Claim(ctx context.Context, originKey string) *StringCmd
	CreateKeyspace(ctx context.Context, entity string) *StatusCmd
	CreateTable(ctx context.Context, table, model string, modelArgs []string, properties ...string) *StatusCmd
	DbSize(ctx context.Context, entity string) *IntCmd
	Del(ctx context.Context, keys ...string) *IntCmd
	DelUser(ctx context.Context, username string) *StatusCmd
	DropKeyspace(ctx context.Context, keyspace string) *StatusCmd
	DropTable(ctx context.Context, table string) *StatusCmd
	Exists(ctx context.Context, keys ...string) *IntCmd
	FlushDB(ctx context.Context, entity string) *StatusCmd
	Get(ctx context.Context, key string) *StringCmd
	Heya(ctx context.Context, message string) *StringCmd
	InspectKeyspace(ctx context.Context, keyspace string) *StringSliceCmd
	InspectKeyspaces(ctx context.Context) *StringSliceCmd
	InspectTable(ctx context.Context, table string) *StringSliceCmd
	KeyLen(ctx context.Context, key string) *IntCmd
	LGet(ctx context.Context, key string) *StringSliceCmd
	LGetLimit(ctx context.Context, key string, limit int) *StringSliceCmd
	LGetLen(ctx context.Context, key string) *IntCmd
	LGetValueAt(ctx context.Context, key string, index int) *StringCmd
	LGetFirst(ctx context.Context, key string) *StringCmd
	LGetLast(ctx context.Context, key string) *StringCmd
	LGetRange(ctx context.Context, key string, start, stop int) *StringSliceCmd
	ListUser(ctx context.Context) *StringSliceCmd
	LModPush(ctx context.Context, key string, elements ...interface{}) *StatusCmd
	LModInsert(ctx context.Context, key string, index int, value interface{}) *StatusCmd
	LModPop(ctx context.Context, key string, index int) *StringCmd
	LModRemove(ctx context.Context, key string, index int) *StatusCmd
	LModClear(ctx context.Context, key string) *StatusCmd
	LSet(ctx context.Context, key string, values ...interface{}) *StatusCmd
	LSKeys(ctx context.Context, entity string, limit int) *StringSliceCmd
	MGet(ctx context.Context, keys ...interface{}) *SliceCmd
	MKSnap(ctx context.Context, snapName string) *StatusCmd
	MPop(ctx context.Context, keys ...interface{}) *StringSliceCmd
	MSet(ctx context.Context, keyValuePairs ...interface{}) *IntCmd
	MUpdate(ctx context.Context, keyValuePairs ...interface{}) *IntCmd
	Pop(ctx context.Context, key string) *StringCmd
	Restore(ctx context.Context, originKey string, username string) *StringCmd
	SDel(ctx context.Context, keys ...interface{}) *StatusCmd
	Set(ctx context.Context, key interface{}, value interface{}) *StatusCmd
	SSet(ctx context.Context, keyValuePairs ...interface{}) *StatusCmd
	SUpdate(ctx context.Context, keyValuePairs ...interface{}) *StatusCmd
	SysInfo(ctx context.Context, property string) *StringCmd
	SysMetric(ctx context.Context, metric string) *StringCmd
	Update(ctx context.Context, key interface{}, value interface{}) *StatusCmd
	Use(ctx context.Context, entity string) *StatusCmd
	USet(ctx context.Context, keyValuePairs ...interface{}) *IntCmd
	WhereAmI(ctx context.Context) *StringSliceCmd
	WhoAmI(ctx context.Context) *StringCmd
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

// Login Attempts to log in using the provided credentials
//
// Time complexity: O(1)
//
// Operation can throw error.
//   - 10	 Bad credentials	The authn credentials are invalid
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
//   - 10	 Bad credentials	The authn credentials are invalid
func (c statefulCmdable) Logout(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd(ctx, "AUTH", "LOGOUT")
	_ = c(ctx, cmd)
	return cmd
}

// AddUser Attempts to create a new user with the provided username, returning the token.
//
// Time complexity: O(1)
//
// Operation can throw error.
//   - 11	 Authn realm error	The current user is not allowed to perform the action
func (c cmdable) AddUser(ctx context.Context, username string) *StringCmd {
	cmd := NewStringCmd(ctx, "AUTH", "ADDUSER", username)
	_ = c(ctx, cmd)
	return cmd
}

// Claim Attempts to claim the root account using the origin key.
//
// Time complexity: O(1)
//
// Operation can throw error.
//   - 10	 Bad credentials	The authn credentials are invalid
func (c cmdable) Claim(ctx context.Context, originKey string) *StringCmd {
	cmd := NewStringCmd(ctx, "AUTH", "CLAIM", originKey)
	_ = c(ctx, cmd)
	return cmd
}

// CreateKeyspace creates a new keyspace.
//
// Transactional: Not yet
// Time complexity: O(1)
//
// Operation can throw error.
//   - string "err-already-exists" if it already existed
//   - 5	Server Error	An error occurred on the server side
func (c cmdable) CreateKeyspace(ctx context.Context, entity string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "CREATE", entity)
	_ = c(ctx, cmd)
	return cmd
}

// CreateTable creates a new table.
//
// Transactional: Not yet
// Time complexity: O(1)
//
// Currently only keymap model is supported.
//
// The keymap model
// A keymap is like an associated array: it maps a key to a value.
// More importantly, it maps a specific key type to a specific value type.
//
// Warning: Everything after CREATE TABLE is case sensitive!
//
// This is how you create keymap tables:
//
// CREATE TABLE <entity> keymap(<type>,<type>) <properties>
//
// Operation can throw error.
//   - string "err-already-exists" if it already existed
//   - string "default-container-unset" if the connection level default keyspace has not been set
//   - 5	Server error	An error occurred on the server side
func (c cmdable) CreateTable(ctx context.Context, table, model string, modelArgs []string, properties ...string) *StatusCmd {
	args := make([]interface{}, 4, len(properties)+4)
	args[0] = "CREATE"
	args[1] = "TABLE"
	args[2] = table
	args[3] = model + "(" + strings.Join(modelArgs, ",") + ")"
	for _, prop := range properties {
		args = append(args, prop)
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// DbSize Check the number of entries stored in the current table or in the provided entity.
//
// Time complexity: O(1)
func (c cmdable) DbSize(ctx context.Context, entity string) *IntCmd {
	args := make([]interface{}, 1, 2)
	args[0] = "DBSIZE"
	if entity != "" {
		args = append(args, entity)
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// Del Delete 'n' keys from the current table.
// DEL <key1> <key2> ... <keyN>
// This will return the number of keys that were deleted as an unsigned integer
//
// Time complexity: O(n)
//
// Operation can throw error.
//   - 5	Server error	An error occurred on the server side
func (c cmdable) Del(ctx context.Context, keys ...string) *IntCmd {
	args := make([]interface{}, 1, 1+len(keys))
	args[0] = "DEL"
	args = append(args, keys)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// DelUser Attempts to delete the user with the provided username.
//
// Time complexity: O(1)
//
// Operation can throw error.
//   - 10	 Bad credentials	The authn credentials are invalid
//   - 11	 Authn realm error	The current user is not allowed to perform the action
func (c cmdable) DelUser(ctx context.Context, username string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "AUTH", "DELUSER", username)
	_ = c(ctx, cmd)
	return cmd
}

// DropKeyspace removes the specified keyspace from the server.
//
// Operation can throw error.
//   - string "container-not-found "if the keyspace wasn't found
//   - string "still-in-use" if clients are still connected to the keyspace or the keyspace is not empty
//   - 5	Server error	An error occurred on the server side
func (c cmdable) DropKeyspace(ctx context.Context, keyspace string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "DROP", "KEYSPACE", keyspace)
	_ = c(ctx, cmd)
	return cmd
}

// DropTable removes the specified table from the keyspace.
//
// Operation can throw error.
//   - string "container-not-found" if the keyspace wasn't found
//   - string "still-in-use" if clients are still connected to the table
//   - string "default-container-unset" if the connection level default keyspace has not been set
//   - 5	Server error	An error occurred on the server side
func (c cmdable) DropTable(ctx context.Context, table string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "DROP", "TABLE", table)
	_ = c(ctx, cmd)
	return cmd
}

// Exists Check if 'n' keys exist in the current table.
// EXISTS <key1> <key2> ... <keyN>
// This will return the number of keys that exist as an unsigned integer.
//
// Time complexity: O(n)
func (c cmdable) Exists(ctx context.Context, keys ...string) *IntCmd {
	args := make([]interface{}, 1, 1+len(keys))
	args[0] = "EXISTS"
	args = append(args, keys)
	cmd := NewIntCmd(ctx, args...)
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
//   - 5 Server error	- An error occurred on the server side
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
// Operation can throw error.
//   - 1	Nil	The client asked for a non-existent object
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
//
// Example:
// 		ctx := context.Background()
//
//    sdb := skytable.NewClient(&skytable.Options{
//			Addr: "localhost:2003",
//		})
//
//		reply := sdb.Heya(ctx, "").Result()
//		fmt.Println(reply)
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

// InspectKeyspace This will return a flat array with all the table names
// passing keyspace as empty string "" will return all the table names in current keyspace
func (c cmdable) InspectKeyspace(ctx context.Context, keyspace string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "INSPECT", "KEYSPACE", keyspace)
	_ = c(ctx, cmd)
	return cmd
}

// InspectKeyspaces This will return a flat array with all the keyspace names
func (c cmdable) InspectKeyspaces(ctx context.Context) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "INSPECT", "KEYSPACES")
	_ = c(ctx, cmd)
	return cmd
}

// InspectTable This will return a string with the table's syntactical description.
// For example, the keymap model can return:
// Keymap { data: (binstr,binstr), volatile: true }
func (c cmdable) InspectTable(ctx context.Context, table string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "INSPECT", "TABLE", table)
	_ = c(ctx, cmd)
	return cmd
}

// KeyLen Returns the length of the UTF-8 string, if it exists in the current table.
//
// Time complexity: O(1)
//
// Operation can throw error.
//   - 1	Nil	The client asked for a non-existent object
func (c cmdable) KeyLen(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd(ctx, "KEYLEN", key)
	_ = c(ctx, cmd)
	return cmd
}

// LGet Returns all the values contained in the provided list, if it exists in the current table.
//
// Time complexity: O(n)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
func (c cmdable) LGet(ctx context.Context, key string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "LGET", key)
	_ = c(ctx, cmd)
	return cmd
}

// LGetLimit Returns a maximum of limit values from the provided list, if it exists in the current table
//
// Time complexity: O(n)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
func (c cmdable) LGetLimit(ctx context.Context, key string, limit int) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "LGET", key, "limit", limit)
	_ = c(ctx, cmd)
	return cmd
}

// LGetLen Returns the length of the list
//
// Time complexity: O(1)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
func (c cmdable) LGetLen(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd(ctx, "LGET", key, "len")
	_ = c(ctx, cmd)
	return cmd
}

// LGetValueAt Returns the element present at the provided index, if it exists in the given list.
//
// Time complexity: O(1)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
//	- string "bad-list-index"	The index is out of range
func (c cmdable) LGetValueAt(ctx context.Context, key string, index int) *StringCmd {
	cmd := NewStringCmd(ctx, "LGET", key, "valueat", index)
	_ = c(ctx, cmd)
	return cmd
}

// LGetFirst Returns the first element present in the list, if it exists.
//
// Time complexity: O(1)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
//  - string "list-is-empty"
func (c cmdable) LGetFirst(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd(ctx, "LGET", key, "first")
	_ = c(ctx, cmd)
	return cmd
}

// LGetLast Returns the last element present in the list, if it exists.
//
// Time complexity: O(1)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
//  - string "list-is-empty"
func (c cmdable) LGetLast(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd(ctx, "LGET", key, "last")
	_ = c(ctx, cmd)
	return cmd
}

// LGetRange Returns items in the given range.
// If stop is provided as -1, all the elements from that index are returned.
// If a value for stop is provided, then a subarray is returned
// array[start:stop] -> [start, stop)
//
// Time complexity: O(n)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
//	- string "bad-list-index"	The index is out of range
func (c cmdable) LGetRange(ctx context.Context, key string, start int, stop int) *StringSliceCmd {
	args := make([]interface{}, 0, 5)
	args = append(args, "LGET", key, "range", start)
	if stop > -1 {
		args = append(args, stop)
	}

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// ListUser Attempts to return a list of users for the current database instance
//
// Time complexity: O(1)
func (c cmdable) ListUser(ctx context.Context) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "AUTH", "LISTUSER")
	_ = c(ctx, cmd)
	return cmd
}

// LModPush Appends the elements to the end of the provided list, if it exists.
//
// Time complexity: O(1)
//
// Operation can throw error.
// 	- 1  Nil	The client asked for a non-existent object
//  - 5  Server error  An error occurred on the server side
func (c cmdable) LModPush(ctx context.Context, key string, elements ...interface{}) *StatusCmd {
	args := make([]interface{}, 0, 3+len(elements))
	args = append(args, "LMOD", key, "push")
	args = append(args, elements...)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// LModInsert Inserts the element to the provided index,
// if it is valid while shifting elements to the right if required.
//
// Time complexity: O(1)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
//  - 5 Server error  An error occurred on the server side
//	- string "bad-list-index"	The index is out of range
func (c cmdable) LModInsert(ctx context.Context, key string, index int, value interface{}) *StatusCmd {
	cmd := NewStatusCmd(ctx, "LMOD", key, "insert", index, value)
	_ = c(ctx, cmd)
	return cmd
}

// LModPop Removes the element from the end of the list if index<0 or from the provided index
// while shifting elements to the right if required.
//
// Time complexity: O(1)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
//  - 5 Server error  An error occurred on the server side
//  - string "bad-list-index"	The index is out of range
func (c cmdable) LModPop(ctx context.Context, key string, index int) *StringCmd {
	args := make([]interface{}, 0, 4)
	args = append(args, "LMOD", key, "pop")
	if index > 0 {
		args = append(args, index)
	}
	cmd := NewStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// LModRemove Removes the element at the provided index from the list, shifting elements to the right if required.
//
// Time complexity: O(1)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
//  - 5 Server error  An error occurred on the server side
//  - string "bad-list-index"	The index is out of range
func (c cmdable) LModRemove(ctx context.Context, key string, index int) *StatusCmd {
	cmd := NewStatusCmd(ctx, "LMOD", key, "remove", index)
	_ = c(ctx, cmd)
	return cmd
}

// LModClear Removes all the elements present in the list.
//
// Time complexity: O(n)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
//  - 5 Server error  An error occurred on the server side
func (c cmdable) LModClear(ctx context.Context, key string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "LMOD", key, "clear")
	_ = c(ctx, cmd)
	return cmd
}

// LSet Creates a list with the provided values,
// or simply creates an empty list if it doesn't already exist in the table.
//
// Time complexity: O(n)
//
// Operation can throw error.
// 	- 1 Nil	The client asked for a non-existent object
//  - 5 Server error  An error occurred on the server side
func (c cmdable) LSet(ctx context.Context, key string, values ...interface{}) *StatusCmd {
	args := make([]interface{}, 0, 2+len(values))
	args = append(args, "LSET", key)
	args = append(args, values...)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// LSKeys Returns a flat string array of keys present in the current table or in the provided entity.
// If no <limit> is given, then a maximum of 10 keys are returned.
// If a limit is specified, then a maximum of <limit> keys are returned. The order of keys is meaningless.
// For current table pass entity as ""
// For default limit 10, you can pass limit as "0"
//
// Time complexity: O(n)
func (c cmdable) LSKeys(ctx context.Context, entity string, limit int) *StringSliceCmd {
	args := make([]interface{}, 0, 3)
	args = append(args, "LSKEYS")
	if entity != "" {
		args = append(args, entity)
	}
	if limit > 0 {
		args = append(args, limit)
	}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// MGet Get the value of 'n' keys from the current table, if they exist.
//
// Time complexity: O(n)
func (c cmdable) MGet(ctx context.Context, keys ...interface{}) *SliceCmd {
	args := make([]interface{}, 0, 1+len(keys))
	args = append(args, "MGET", keys)
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// MKSnap This action can be used to create a snapshot.
// Do note that this action requires snapshotting to be enabled on the server side,
// before it can create snapshots. If you want to create snapshots without snapshots being enabled
// on the server-side, pass a second argument <SNAPNAME> to specify a snapshot name and a snapshot will
// be created in a folder called rsnap under your data directory. For more information on snapshots,
// read this document https://docs.skytable.io/snapshots
//
// Time complexity: O(n)
//
// Operation can throw error.
// 	- string "err-snapshot-disabled" Snapshots have been disabled on the server-side
//	- string "err-snapshot-busy" A snapshot operation is already in progress
func (c cmdable) MKSnap(ctx context.Context, snapName string) *StatusCmd {
	args := make([]interface{}, 0, 2)
	args = append(args, "MKSNAP")
	if snapName != "" {
		args = append(args, snapName)
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// MPop Deletes and returns the values of the provided 'n' keys from the current table.
// If the database is poisoned, this will return a server error
//
// Time complexity: O(n)
//
// Operation can throw error.
// 	- 5	Server error	An error occurred on the server side
func (c cmdable) MPop(ctx context.Context, keys ...interface{}) *StringSliceCmd {
	args := make([]interface{}, 0, 1+len(keys))
	args = append(args, "MOP", keys)
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// MSet Set the value of 'n' keys in the current table, if they don't already exist.
// This will return the number of keys that were set as an unsigned integer.
//
// Time complexity: O(n)
//
// Operation can throw error.
// 	- 5	Server error	An error occurred on the server side
func (c cmdable) MSet(ctx context.Context, keyValuePairs ...interface{}) *IntCmd {
	args := make([]interface{}, 0, 1+len(keyValuePairs))
	args = append(args, "MSET", keyValuePairs)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// MUpdate Update the value of 'n' keys in the current table, if they already exist.
// This will return the number of keys that were updated as an unsigned integer.
//
// Time complexity: O(n)
//
// Operation can throw error.
// 	- 5	Server error	An error occurred on the server side
func (c cmdable) MUpdate(ctx context.Context, keyValuePairs ...interface{}) *IntCmd {
	args := make([]interface{}, 0, 1+len(keyValuePairs))
	args = append(args, "MUPDATE", keyValuePairs)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// Pop Deletes and return the value of the provided key from the current table.
// If the database is poisoned, this will return a server error.
//
// Time complexity: O(1)
//
// Operation can throw error.
// 	- 5	Server error	An error occurred on the server side
func (c cmdable) Pop(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd(ctx, "POP", key)
	_ = c(ctx, cmd)
	return cmd
}

// Restore Attempts to restore the password for the provided user.
// This will regenerate the token and return the newly issued token.
// However, if you aren't a root account, that is, you lost your root password,
// then you'll need to run with username as "root".
//
// Time complexity: O(1)
//
// Operation can throw error.
//   - 10	 Bad credentials	The authn credentials are invalid
//   - 11	 Authn realm error	The current user is not allowed to perform the action
func (c cmdable) Restore(ctx context.Context, originKey, username string) *StringCmd {
	var cmd *StringCmd
	if originKey == "" {
		cmd = NewStringCmd(ctx, "RESTORE", username)
	} else {
		cmd = NewStringCmd(ctx, "RESTORE", originKey, username)
	}
	_ = c(ctx, cmd)
	return cmd
}

// SDel Delete all keys if all of the keys exist in the current table.
// Do note that if a single key doesn't exist, then a Nil code is returned.
//
// Time complexity: O(n)
//
// Operation can throw error.
//  - 1 Nil	The client asked for a non-existent object
// 	- 5	Server error	An error occurred on the server side
func (c cmdable) SDel(ctx context.Context, keys ...interface{}) *StatusCmd {
	args := make([]interface{}, 0, 1+len(keys))
	args = append(args, "SDEL", keys)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// Set the value of a key in the current table, if it doesn't already exist
// Throws overwriting error if the key already exists.
//
// Time complexity: O(1)
//
// Operation can throw error.
//   - 2	Overwrite error	The client tried to overwrite data
//   - 5	Server error	  An error occurred on the server side
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

// SSet Set all keys to the given values only if all of them don't exist in the current table
//
// Time complexity: O(n)
//
// Operation can throw error.
// - 2	Overwrite error	The client tried to overwrite data
// - 5	Server error	  An error occurred on the server side
func (c cmdable) SSet(ctx context.Context, keyValuePairs ...interface{}) *StatusCmd {
	args := make([]interface{}, 0, 1+len(keyValuePairs))
	args = append(args, "SSET", keyValuePairs)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SUpdate Update all keys if all of the keys exist in the current table.
// Do note that if a single key doesn't exist, then a Nil code is returned.
//
// Time complexity: O(n)
//
// Operation can throw error.
// - 1 Nil	The client asked for a non-existent object
// - 5	Server error	An error occurred on the server side
func (c cmdable) SUpdate(ctx context.Context, keyValuePairs ...interface{}) *StatusCmd {
	args := make([]interface{}, 0, 1+len(keyValuePairs))
	args = append(args, "SUPDATE", keyValuePairs)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// SysInfo Returns static properties of the system, i.e properties that do not change during runtime.
//
// The following properties are available:
// 	- version: Returns the server version (String)
// 	- protocol: Returns the protocol version string (String)
// 	- protover: Returns the protocol version (float)
//
// Time complexity: O(1)
func (c cmdable) SysInfo(ctx context.Context, property string) *StringCmd {
	cmd := NewStringCmd(ctx, "SYS", "INFO", property)
	_ = c(ctx, cmd)
	return cmd
}

// SysMetric Returns dynamic properties of the system, i.e metrics are properties that can change during runtime.
//
// The following metrics are available:
// 	- health: Returns "good" or "critical" depending on the system state (String)
// 	- storage: Returns bytes used for on-disk storage (uint64)
func (c cmdable) SysMetric(ctx context.Context, metric string) *StringCmd {
	cmd := NewStringCmd(ctx, "SYS", "METRIC", metric)
	_ = c(ctx, cmd)
	return cmd
}

// Update the value of an existing key in the current table
//
// Time complexity: O(1)
//
// Operation can throw error.
//   - 1	Nil	The client asked for a non-existent object
//   - 5	Server error	An error occurred on the server side
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

// Use the specified entity - table or keyspace
// Entity is simply a string in FQE i.e. <keyspace>:<table> for a table or <keyspace> for a keyspace.
// See https://docs.skytable.io/containers for more.
//
// Time complexity: O(1)
//
// The operation can throw error.
// 	  - string "container-not-found" if the keyspace wasn't found
// 	  - string "default-container-unset" if the connection level default keyspace has not been set
//
// Example:
//    ctx := context.Background()
//
//    sdb := skytable.NewClient(&skytable.Options{
//      Addr: "localhost:2003",
//    })
//
//    // creating new keyspace with the name "keyspace"
//    if err := sdb.CreateKeyspace(ctx, "keyspace"); err != nil {
//			panic(err)
//		}
//
//		// creating new table under the keyspace "keyspace" with the name "table"
//    if err := sdb.CreateTable(ctx, "keyspace:table", "keymap", []string{"str", "binstr"}).Err(); err != nil {
//			panic(err)
//		}
//
//		// using the entity "keyspace:table"
//   	if err := sdb.Use(ctx, "keyspace:table"); err != nil {
//			panic(err)
//		}
func (c cmdable) Use(ctx context.Context, entity string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "USE", entity)
	_ = c(ctx, cmd)
	return cmd
}

// USet SET all keys if they don't exist, or UPDATE them if they do exist.
// This operation performs USETs in the current table
//
// Time complexity: O(n)
//
// Operation can throw error.
// - 5  Server error	 An error occurred on the server side
func (c cmdable) USet(ctx context.Context, keyValuePairs ...interface{}) *IntCmd {
	args := make([]interface{}, 0, 1+len(keyValuePairs))
	args = append(args, "USET", keyValuePairs)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// WhereAmI Returns an array with either the name of the current keyspace as the first element
// or if a default table is set, then it returns the keyspace name as the first element
// and the table name as the second element
func (c cmdable) WhereAmI(ctx context.Context) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "WHEREAMI")
	_ = c(ctx, cmd)
	return cmd
}

// WhoAmI Returns a string with the AuthID of the currently logged-in user
// or errors if the user is not logged in
//
// Time complexity: O(1)
func (c cmdable) WhoAmI(ctx context.Context) *StringCmd {
	cmd := NewStringCmd(ctx, "AUTH", "WHOAMI")
	_ = c(ctx, cmd)
	return cmd
}
