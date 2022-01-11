package armadaerrors

import (
	"context"
	"strings"
	"testing"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gotest.tools/v3/assert"

	"github.com/G-Research/armada/internal/common/requestid"
)

func TestCodeFromError(t *testing.T) {
	tests := map[string]struct {
		err  error
		want codes.Code
	}{
		"ErrAlreadyExists":                {&ErrAlreadyExists{}, codes.AlreadyExists},
		"ErrNotFound":                     {&ErrNotFound{}, codes.NotFound},
		"ErrInvalidArgument":              {&ErrInvalidArgument{}, codes.InvalidArgument},
		"pkg.Error => ErrAlreadyExists":   {errors.WithMessage(&ErrAlreadyExists{}, "foo"), codes.AlreadyExists},
		"pkg.Error => ErrNotFound":        {errors.WithMessage(&ErrNotFound{}, "foo"), codes.NotFound},
		"pkg.Error => ErrInvalidArgument": {errors.WithMessage(&ErrInvalidArgument{}, "foo"), codes.InvalidArgument},
		"pkg.Error":                       {errors.New("foo"), codes.Unknown},
		"nil":                             {nil, codes.OK},
		"gRPC status":                     {status.New(codes.Internal, "foo").Err(), codes.Internal},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := CodeFromError(tc.err)
			if got != tc.want {
				assert.Equal(t, tc.want, got, "expected %v but got %v", tc.want, got)
			}
		})
	}
}

func TestUnaryServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	var handlerErr error
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, handlerErr
	}
	f := UnaryServerInterceptor()

	// nils should be passed through as-is
	handlerErr = nil
	_, err := f(ctx, nil, nil, handler)
	assert.NilError(t, err)

	// gRPC-style errors should be passed through as-is
	handlerErr = status.Error(codes.Aborted, "foo")
	_, err = f(ctx, nil, nil, handler)
	st, ok := status.FromError(err)
	assert.Assert(t, ok)
	assert.Equal(t, codes.Aborted, st.Code(), "expected %v, but got %v", codes.Aborted, st.Code())

	// a chain of errors should result in the message of the cause error being returned
	innerErr := &ErrAlreadyExists{}
	handlerErr = errors.WithMessage(innerErr, "foo")
	_, err = f(ctx, nil, nil, handler)
	st, ok = status.FromError(err)
	assert.Assert(t, ok)
	assert.Equal(t, codes.AlreadyExists, st.Code(), "expected %v, but got %v", codes.AlreadyExists, st.Code())
	assert.Equal(t, st.Message(), innerErr.Error(), "expected %q, but got %q", st.Message(), innerErr.Error())

	// if the context contains a request id, it should be included in the error message
	id := "123"
	ctx, ok = requestid.AddToIncomingContext(ctx, id)
	assert.Assert(t, ok)
	_, err = f(ctx, nil, nil, handler)
	st, ok = status.FromError(err)
	assert.Assert(t, ok)
	assert.Assert(t, strings.Contains(st.Message(), id), "expected error message to contain %q, but got %q", id, st.Message())
}

func TestStreamServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	stream := &grpc_middleware.WrappedServerStream{}
	stream.WrappedContext = ctx
	var handlerErr error
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		return handlerErr
	}
	f := StreamServerInterceptor()

	// nils should be passed through as-is
	handlerErr = nil
	err := f(nil, stream, nil, handler)
	assert.NilError(t, err)

	// gRPC-style errors should be passed through as-is
	handlerErr = status.Error(codes.Aborted, "foo")
	err = f(nil, stream, nil, handler)
	st, ok := status.FromError(err)
	assert.Assert(t, ok)
	assert.Equal(t, codes.Aborted, st.Code(), "expected %v, but got %v", codes.Aborted, st.Code())

	// a chain of errors should result in the message of the cause error being returned
	innerErr := &ErrAlreadyExists{}
	handlerErr = errors.WithMessage(innerErr, "foo")
	err = f(nil, stream, nil, handler)
	st, ok = status.FromError(err)
	assert.Assert(t, ok)
	assert.Equal(t, codes.AlreadyExists, st.Code(), "expected %v, but got %v", codes.AlreadyExists, st.Code())
	assert.Equal(t, innerErr.Error(), st.Message(), "expected %v, but got %v", innerErr.Error(), st.Message())

	// if the context contains a request id, it should be included in the error message
	id := "123"
	ctx, ok = requestid.AddToIncomingContext(ctx, id)
	assert.Assert(t, ok)
	stream.WrappedContext = ctx
	err = f(nil, stream, nil, handler)
	st, ok = status.FromError(err)
	assert.Assert(t, ok)
	assert.Assert(t, strings.Contains(st.Message(), id), "expected error message to contain %q, but got %q", id, st.Message())
}
