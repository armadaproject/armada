// Package armadaerrors contains generic errors that should be returned by code handling gRPC requests.
// gRPC interceptors will look for the error types defined in this file and automatically set
// the gRPC status and return code correctly.
//
// If multiple errors occur in some function (e.g., if multiple queues already exists), that
// function should return an error of type multierror.Error from package
// github.com/hashicorp/go-multierror that encapsulates those individual errors.
package armadaerrors

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/common/requestid"
)

// ErrNoPermission represents an error that occurs when a client tries to perform some action
// through the gRPC API for which it does not have permissions.
//
// It may be necessary populate the Action field by recovering this error at the gRPC endpoint (using errors.As)
// and updating the field in-place.
type ErrNoPermission struct {
	// Principal that attempted the action
	Principal string
	// The missing permission
	Permission string
	// The attempted action
	Action string
	// Optional message included with the error message
	Message string
}

// retryablePostgresErrors represents set of postgres errors that can be retried. Fundamentally these are all
//issues with postgres itself, with the network or with authentication
var retryablePostgresErrors = map[string]bool{

	// Connection issues
	pgerrcode.ConnectionException:                           true,
	pgerrcode.ConnectionDoesNotExist:                        true,
	pgerrcode.ConnectionFailure:                             true,
	pgerrcode.SQLClientUnableToEstablishSQLConnection:       true,
	pgerrcode.SQLServerRejectedEstablishmentOfSQLConnection: true,
	pgerrcode.TransactionResolutionUnknown:                  true,

	// Authorization issues
	pgerrcode.InvalidAuthorizationSpecification: true,
	pgerrcode.InvalidPassword:                   true,

	// Access Rule Violation
	pgerrcode.InsufficientPrivilege: true,

	// Coding error with the query/schema mismatch
	pgerrcode.SyntaxErrorOrAccessRuleViolation:   true,
	pgerrcode.SyntaxError:                        true,
	pgerrcode.CannotCoerce:                       true,
	pgerrcode.GroupingError:                      true,
	pgerrcode.WindowingError:                     true,
	pgerrcode.InvalidRecursion:                   true,
	pgerrcode.InvalidForeignKey:                  true,
	pgerrcode.InvalidName:                        true,
	pgerrcode.NameTooLong:                        true,
	pgerrcode.ReservedName:                       true,
	pgerrcode.DatatypeMismatch:                   true,
	pgerrcode.IndeterminateDatatype:              true,
	pgerrcode.CollationMismatch:                  true,
	pgerrcode.IndeterminateCollation:             true,
	pgerrcode.WrongObjectType:                    true,
	pgerrcode.GeneratedAlways:                    true,
	pgerrcode.UndefinedColumn:                    true,
	pgerrcode.UndefinedFunction:                  true,
	pgerrcode.UndefinedTable:                     true,
	pgerrcode.UndefinedParameter:                 true,
	pgerrcode.UndefinedObject:                    true,
	pgerrcode.DuplicateColumn:                    true,
	pgerrcode.DuplicateCursor:                    true,
	pgerrcode.DuplicateDatabase:                  true,
	pgerrcode.DuplicateFunction:                  true,
	pgerrcode.DuplicatePreparedStatement:         true,
	pgerrcode.DuplicateSchema:                    true,
	pgerrcode.DuplicateTable:                     true,
	pgerrcode.DuplicateAlias:                     true,
	pgerrcode.DuplicateObject:                    true,
	pgerrcode.AmbiguousColumn:                    true,
	pgerrcode.AmbiguousFunction:                  true,
	pgerrcode.AmbiguousParameter:                 true,
	pgerrcode.AmbiguousAlias:                     true,
	pgerrcode.InvalidColumnReference:             true,
	pgerrcode.InvalidColumnDefinition:            true,
	pgerrcode.InvalidCursorDefinition:            true,
	pgerrcode.InvalidDatabaseDefinition:          true,
	pgerrcode.InvalidFunctionDefinition:          true,
	pgerrcode.InvalidPreparedStatementDefinition: true,
	pgerrcode.InvalidSchemaDefinition:            true,
	pgerrcode.InvalidTableDefinition:             true,
	pgerrcode.InvalidObjectDefinition:            true,

	// Resource issues
	pgerrcode.InsufficientResources:      true,
	pgerrcode.DiskFull:                   true,
	pgerrcode.OutOfMemory:                true,
	pgerrcode.TooManyConnections:         true,
	pgerrcode.ConfigurationLimitExceeded: true,

	// Operator issues
	pgerrcode.OperatorIntervention: true,
	pgerrcode.QueryCanceled:        true,
	pgerrcode.AdminShutdown:        true,
	pgerrcode.CrashShutdown:        true,
	pgerrcode.CannotConnectNow:     true,
	pgerrcode.DatabaseDropped:      true,

	// External errors
	pgerrcode.SystemError:   true,
	pgerrcode.IOError:       true,
	pgerrcode.UndefinedFile: true,
	pgerrcode.DuplicateFile: true,

	// Internal Errors
	pgerrcode.InternalError:  true,
	pgerrcode.DataCorrupted:  true,
	pgerrcode.IndexCorrupted: true,
}

func (err *ErrNoPermission) Error() (s string) {
	if err.Action != "" {
		s = fmt.Sprintf("%s lacks permission %s required for action %s", err.Principal, err.Permission, err.Action)
	} else {
		s = fmt.Sprintf("%s lacks permission %s", err.Principal, err.Permission)
	}
	if err.Message != "" {
		s = s + fmt.Sprintf("; %s", err.Message)
	}
	return
}

// ErrAlreadyExists is a generic error to be returned whenever some resource already exists.
// Type and Message are optional and are omitted from the error message if not provided.
type ErrAlreadyExists struct {
	Type    string // Resource type, e.g., "queue" or "user"
	Value   string // Resource name, e.g., "Bob"
	Message string // An optional message to include in the error message
}

func (err *ErrAlreadyExists) Error() (s string) {
	if err.Type != "" {
		s = fmt.Sprintf("resource %q of type %q already exists", err.Value, err.Type)
	} else {
		s = fmt.Sprintf("resource %q already exists", err.Value)
	}
	if err.Message != "" {
		return s + fmt.Sprintf("; %s", err.Message)
	} else {
		return s
	}
}

// ErrNotFound is a generic error to be returned whenever some resource isn't found.
// Type and Message are optional and are omitted from the error message if not provided.
//
// See ErrAlreadyExists for more info.
type ErrNotFound struct {
	Type    string
	Value   string
	Message string
}

func (err *ErrNotFound) Error() (s string) {
	if err.Type != "" {
		s = fmt.Sprintf("resource %q of type %q does not exist", err.Value, err.Type)
	} else {
		s = fmt.Sprintf("resource %q does not exist", err.Value)
	}
	if err.Message != "" {
		return s + fmt.Sprintf("; %s", err.Message)
	} else {
		return s
	}
}

// ErrInvalidArgument is a generic error to be returned on invalid argument.
// Message is optional and is omitted from the error message if not provided.
type ErrInvalidArgument struct {
	Name    string      // Name of the field referred to, e.g., "priorityFactor"
	Value   interface{} // The invalid value that was provided
	Message string      // An optional message to include with the error message, e.g., explaining why the value is invalid
}

func (err *ErrInvalidArgument) Error() string {
	if err.Message == "" {
		return fmt.Sprintf("value %q is invalid for field %q", err.Value, err.Name)
	} else {
		return fmt.Sprintf("value %q is invalid for field %q; %s", err.Value, err.Name, err.Message)
	}
}

// ErrMaxRetriesExceeded is an error that indicates we have retried an operation so many times that we have given up
// The internal error should contain the last error before giving up
type ErrMaxRetriesExceeded struct {
	Message   string
	LastError error
}

func (e *ErrMaxRetriesExceeded) Error() string {
	if e.Message == "" {
		return e.LastError.Error()
	}
	if e.LastError == nil {
		return e.Message
	}
	return fmt.Sprintf("%s: %s", e.Message, e.LastError.Error())
}

func (e *ErrMaxRetriesExceeded) Unwrap() error {
	return e.LastError
}

// CodeFromError maps error types to gRPC return codes.
// Uses errors.As to look through the chain of errors, as opposed to just considering the topmost error in the chain.
func CodeFromError(err error) codes.Code {

	// Check if the error is a gRPC status and, if so, return the embedded code.
	// If the error is nil, this returns an OK status code.
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}

	// Otherwise, we check for known error types.
	// Using {} scopes just to re-use the "e" variable name for each case.
	{
		var e *ErrAlreadyExists
		if errors.As(err, &e) {
			return codes.AlreadyExists
		}
	}
	{
		var e *ErrNotFound
		if errors.As(err, &e) {
			return codes.NotFound
		}
	}
	{
		var e *ErrInvalidArgument
		if errors.As(err, &e) {
			return codes.InvalidArgument
		}
	}
	{
		var e *ErrPodUnschedulable
		if errors.As(err, &e) {
			return codes.InvalidArgument
		}
	}

	return codes.Unknown
}

var NATS_CONNECTION_ERRORS = []error{
	nats.ErrConnectionClosed,
	nats.ErrConnectionDraining,
	nats.ErrDrainTimeout,
	nats.ErrConnectionReconnecting,
	nats.ErrTimeout,
	nats.ErrBadTimeout,
	nats.ErrNoServers,
	nats.ErrDisconnected,
}

var PULSAR_CONNECTION_ERRORS = []pulsar.Result{
	pulsar.TimeoutError,
	pulsar.LookupError,
	pulsar.ConnectError,
	pulsar.ReadError,
	pulsar.NotConnectedError,
	pulsar.TooManyLookupRequestException,
	pulsar.ServiceUnitNotReady,
	pulsar.ProducerQueueIsFull,
}

// IsNetworkError returns true if err is a network-related error.
// If err is an error chain, this function returns true if any error in the chain is a network error.
//
// For details, see
// https://stackoverflow.com/questions/22761562/portable-way-to-detect-different-kinds-of-network-error
func IsNetworkError(err error) bool {

	// Return immediately on nil.
	if err == nil {
		return false
	}

	// Because deadline exceeded is typically caused by a network timeout, we consider it a network error.
	if ok := errors.Is(err, context.DeadlineExceeded); ok {
		return true
	}

	// EOF indicates a network termination
	if errors.Is(err, io.EOF) {
		return true
	}

	// Generic network errors in the net package. Redis returns these.
	{
		var e net.Error
		if ok := errors.As(err, &e); ok {
			return true
		}
	}
	{
		var e *net.OpError
		if ok := errors.As(err, &e); ok {
			return true
		}
	}

	// Generic syscall errors.
	// Not sure if anything returns this, but it seems proper to check.
	{
		var e syscall.Errno
		if ok := errors.As(err, &e); ok {
			if e == syscall.ECONNREFUSED {
				return true
			} else if e == syscall.ECONNRESET {
				return true
			} else if e == syscall.ECONNABORTED {
				return true
			}
		}
	}

	// Errors associated with connection problems with Nats.
	for _, e := range NATS_CONNECTION_ERRORS {
		if ok := errors.Is(err, e); ok {
			return true
		}
	}

	// Errors associated with connection problems with Pulsar.
	{
		var e *pulsar.Error
		if ok := errors.As(err, &e); ok {
			fmt.Println("Got Pulsar error", e)
			for _, result := range PULSAR_CONNECTION_ERRORS {
				if e.Result() == result {
					return true
				}
			}
		}
	}

	// Pulsar subscribe returns an errors.errorString with a particular message
	// (as opposed to using its internal error type).
	if e := errors.Cause(err); e != nil {
		if strings.Contains(e.Error(), "connection error") { // Pulsar subscribe
			return true
		}
	}

	return false
}

// UnaryServerInterceptor returns an interceptor that extracts the cause of an error chain
// and returns it as a gRPC status error. It also limits the number of characters returned.
//
// To log the full error chain and return only the cause to the user, insert this interceptor before
// the logging interceptor.
func UnaryServerInterceptor(maxErrorSize uint) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		rv, err := handler(ctx, req)

		// If the error is nil or a gRPC status, return as-is.
		// status.FromError(nil) returns true.
		if _, ok := status.FromError(err); ok {
			return rv, err
		}

		// Otherwise, get the cause and convert to a gRPC status error
		cause := errors.Cause(err)
		code := CodeFromError(cause)

		// If available, annotate the status with the request ID
		var errorMessage string
		if id, ok := requestid.FromContext(ctx); ok {
			errorMessage = fmt.Sprintf("[%s: %q] ", requestid.MetadataKey, id) + err.Error()
		} else {
			errorMessage = err.Error()
		}

		// Limit error message size.
		if len(errorMessage) > int(maxErrorSize) {
			errorMessage = errorMessage[:maxErrorSize] + "... (truncated)"
		}

		return rv, status.Error(code, errorMessage)
	}
}

// StreamServerInterceptor returns an interceptor that extracts the cause of an error chain
// and returns it as a gRPC status error. It also limits the number of characters returned.
func StreamServerInterceptor(maxErrorSize uint) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)

		// If the error is nil or a gRPC status, return as-is
		// status.FromError(nil) returns true.
		if _, ok := status.FromError(err); ok {
			return err
		}

		// Otherwise, get the cause and convert to a gRPC status error
		cause := errors.Cause(err)
		code := CodeFromError(cause)

		// If available, annotate the status with the request ID
		var errorMessage string
		if id, ok := requestid.FromContext(stream.Context()); ok {
			errorMessage = fmt.Sprintf("[%s: %q] ", requestid.MetadataKey, id) + err.Error()
		} else {
			errorMessage = err.Error()
		}

		// Limit error message size.
		if len(errorMessage) > int(maxErrorSize) {
			errorMessage = errorMessage[:maxErrorSize] + "... (truncated)"
		}

		return status.Error(code, errorMessage)
	}
}

func IsRetryablePostgresError(err error) bool {

	// Return immediately on nil.
	if err == nil {
		return false
	}

	// PGX will sometimes wrap the underlying error
	cause := unwrapOrOriginal(err)

	if err, ok := cause.(*pgconn.PgError); ok {
		_, ok := retryablePostgresErrors[err.Code]
		return ok
	}
	// Check to see if we have a wrapped network error
	return IsNetworkError(cause)
}

func unwrapOrOriginal(err error) error {
	unwrapped := errors.Unwrap(err)
	if unwrapped == nil {
		return err
	}
	return unwrapped
}

// ErrPodUnschedulable indicates that a pod can't be scheduled on any node type.
type ErrPodUnschedulable struct {
	// Maps the reason for excluding a node type to the number of node types excluded for this reason.
	countFromReason map[string]int
}

// Add updates the internal counter of errors.
func (err *ErrPodUnschedulable) Add(reason string, count int) *ErrPodUnschedulable {
	if err == nil {
		err = &ErrPodUnschedulable{}
	}
	if err.countFromReason == nil {
		err.countFromReason = make(map[string]int)
	}
	err.countFromReason[reason] += count
	return err
}

func (err *ErrPodUnschedulable) Error() string {
	if len(err.countFromReason) == 0 {
		return "can't schedule pod on any node type"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "can't schedule pod on any node type; ")
	i := 0
	for reason, count := range err.countFromReason {
		fmt.Fprintf(&b, "%d node type(s) excluded because %s", count, reason)
		i++
		if i < len(err.countFromReason) {
			fmt.Fprintf(&b, ", ")
		}
	}
	return b.String()
}

// NewCombinedErrPodUnschedulable returns a new ErrPodUnschedulable with
// countFromReasons aggregated over all arguments.
func NewCombinedErrPodUnschedulable(errs ...error) *ErrPodUnschedulable {
	if len(errs) == 0 {
		return nil
	}

	result := &ErrPodUnschedulable{
		countFromReason: make(map[string]int),
	}
	for _, err := range errs {
		if err == nil {
			continue
		}

		// If the error is of type *ErrPodUnschedulable, merge the reasons.
		if e, ok := err.(*ErrPodUnschedulable); ok {
			if len(e.countFromReason) == 0 {
				continue
			}
			for reason, count := range e.countFromReason {
				result.countFromReason[reason] += count
			}
		} else { // Otherwise, add the error message as a reason.
			result.countFromReason[err.Error()] += 1
		}
	}

	if len(result.countFromReason) == 0 {
		return nil
	}
	return result
}
