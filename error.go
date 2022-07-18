package skytable

import (
	"context"
	"io"
	"net"
	"strings"

	"github.com/satvik007/skytable-go/internal/pool"
	"github.com/satvik007/skytable-go/internal/proto"
)

// ErrClosed performs any operation on the closed client will return this error.
var ErrClosed = pool.ErrClosed

type Error interface {
	error

	// skytableError is a no-op function but
	// serves to distinguish types that are Skytable
	// errors from ordinary errors: a type is a
	// Skytable error if it has a SkytableError method.
	SkytableError()
}

// Table of errors
// Error String	Meaning
// Unknown action	The action is not known by the server
// err-snapshot-busy	A snapshot operation is already in progress
// err-snapshot-disabled	Snapshots have been disabled on the server-side
// err-invalid-snapshot-name	The supplied snapshot name has invalid chars
// default-container-unset	The connection level table/keyspace was not set
// container-not-found	The keyspace/table was not found
// still-in-use	The object couldn't be removed because it is still in use
// err-protected-object	The object is not user accessible
// wrong-model	An action was run against the wrong data model
// err-already-exists	The table/keyspace already exists
// not-ready	The table/keyspace is not ready
// transactional-failure	A transactional action failed to execute
// unknown-ddl-query	An unknown DDL query was run
// malformed-expression	The expression in a DDL query was illegal
// unknown-model	A DDL query was run to create a table of an unknown model
// too-many-args	More args than required was passed to a DDL query
// container-name-too-long	The container name was too long
// bad-container-name	The supplied container name has illegal chars
// unknown-inspect-query	An unknown INSPECT query
// unknown-property	An unknown table property was passed to CREATE TABLE
// keyspace-not-empty	The keyspace couldn't be removed because it still has tables
// pipeline-not-supported-yet	Pipelining is not supported in this server version
// err-auth-disabled	Authn/authz is not enabled
// err-auth-already-claimed	The username has already been created
// err-auth-illegal-username	The username is too long or has invalid characters
// err-auth-deluser-fail	The user cannot be removed

var (
	_ Error = proto.SkytableError("")
)

func shouldRetry(err error, retryTimeout bool) bool {
	switch err {
	case io.EOF, io.ErrUnexpectedEOF:
		return true
	case nil, context.Canceled, context.DeadlineExceeded:
		return false
	}

	if v, ok := err.(timeoutError); ok {
		if v.Timeout() {
			return retryTimeout
		}
		return true
	}

	return false
}

func isSkytableError(err error) bool {
	_, ok := err.(proto.SkytableError)
	return ok
}

func isBadConn(err error, allowTimeout bool, addr string) bool {
	switch err {
	case nil:
		return false
	case context.Canceled, context.DeadlineExceeded:
		return true
	}

	if isSkytableError(err) {
		switch {
		case isReadOnlyError(err):
			// Close connections in read only state in case domain addr is used
			// and domain resolves to a different Skytable Server. See #790.
			return true
		case isMovedSameConnAddr(err, addr):
			// Close connections when we are asked to move to the same addr
			// of the connection. Force a DNS resolution when all connections
			// of the pool are recycled
			return true
		default:
			return false
		}
	}

	if allowTimeout {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false
		}
	}

	return true
}

func isReadOnlyError(err error) bool {
	return strings.HasPrefix(err.Error(), "READONLY ")
}

func isMovedSameConnAddr(err error, addr string) bool {
	skytableError := err.Error()
	if !strings.HasPrefix(skytableError, "MOVED ") {
		return false
	}
	return strings.HasSuffix(skytableError, " "+addr)
}

// ------------------------------------------------------------------------------

type timeoutError interface {
	Timeout() bool
}
