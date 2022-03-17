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

	return codes.Unknown
}

// UnaryServerInterceptor returns an interceptor that extracts the cause of an error chain
// and returns it as a gRPC status error.
//
// To log the full error chain and return only the cause to the user, insert this interceptor before
// the logging interceptor.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		rv, err := handler(ctx, req)

		// If the error is nil or a gRPC status, return as-is
		if _, ok := status.FromError(err); ok {
			return rv, err
		}

		// Otherwise, get the cause and convert to a gRPC status error
		cause := errors.Cause(err)
		code := CodeFromError(cause)

		// If available, annotate the status with the request ID
		if id, ok := requestid.FromContext(ctx); ok {
			return rv, status.Error(code, fmt.Sprintf("[%s: %q] ", requestid.MetadataKey, id)+cause.Error())
		}
		return rv, status.Error(code, cause.Error())
	}
}

// StreamServerInterceptor returns an interceptor that extracts the cause of an error chain
// and returns it as a gRPC status error.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, stream)

		// If the error is nil or a gRPC status, return as-is
		if _, ok := status.FromError(err); ok {
			return err
		}

		// Otherwise, get the cause and convert to a gRPC status error
		cause := errors.Cause(err)
		code := CodeFromError(cause)

		// If available, annotate the status with the request ID
		if id, ok := requestid.FromContext(stream.Context()); ok {
			return status.Error(code, fmt.Sprintf("[%s: %q] ", requestid.MetadataKey, id)+cause.Error())
		}
		return status.Error(code, cause.Error())
	}
}
