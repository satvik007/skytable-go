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
	Lget(ctx context.Context, key string, subActions ...interface{}) *Cmd
	ListUser(ctx context.Context) *StringSliceCmd
	Restore(ctx context.Context, originKey string, username string) *StringCmd
	Set(ctx context.Context, key interface{}, value interface{}) *StatusCmd
	Update(ctx context.Context, key interface{}, value interface{}) *StatusCmd
	Use(ctx context.Context, entity string) *StatusCmd
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

// Lget can be used to access the items in a list.
// Through the sub-actions provided by lget, you can access multiple or individual elements in lists.
//
// Time complexity: O(n)
//
// List of supported sub actions
//   - pass nothing `LGET <list>`:
// 			Returns all the values contained in the provided list, if it exists in the current table.
//    	Time complexity: O(n)
//   - pass "limit", "<limit>" `LGET <list> limit <limit>`:
//    	Returns a maximum of limit values from the provided list, if it exists in the current table
//    	Time complexity: O(n)
//   - pass "len" `LGET <list> len`:
//    	Returns the length of the list
//    	Time complexity: O(1)
//   - pass "valueat", "<index>" `LGET <list> valueat <index>`:
//    	Returns the element present at the provided index, if it exists in the given list.
//    	Time complexity: O(1)
//   - pass "first" `LGET <list> first`:
//    	Returns the first element present in the list, if it exists.
//    	Time complexity: O(1)
//   - pass "last" `LGET <list> last`:
//    	Returns the last element present in the list, if it exists.
//    	Time complexity: O(1)
//   - pass "range", "<start>" `LGET <list> range <start>`:
//    	Returns the elements present in the list starting from the provided index, if it exists.
//    	Time complexity: O(n)
//   - pass "range", "<start>", "<end>" `LGET <list> range <start> <stop>`:
//    	Returns items in the given range.
//    	Time complexity: O(n)
//
// Operation can throw error.
//   - 1	Nil	The client asked for a non-existent object
//   - string "bad-list-index" if the index is out of range
//   - string "list-is-empty" if the list is empty and you query for "first" or "last"
func (c cmdable) Lget(ctx context.Context, key string, subActions ...interface{}) *Cmd {
	args := make([]interface{}, 1, 2+len(subActions))
	args[0] = "LGET"
	args[1] = key
	args = append(args, subActions...)
	cmd := NewCmd(ctx, args...)
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

// WhoAmI Returns a string with the AuthID of the currently logged-in user
// or errors if the user is not logged in
//
// Time complexity: O(1)
func (c cmdable) WhoAmI(ctx context.Context) *StringCmd {
	cmd := NewStringCmd(ctx, "AUTH", "WHOAMI")
	_ = c(ctx, cmd)
	return cmd
}
