package requestid

import (
	"context"
	"testing"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/renstrom/shortuuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestAddGet(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))

	// Test adding and getting an id
	id := shortuuid.New()
	ctx, ok := AddToIncomingContext(ctx, id)
	require.True(t, ok, "adding id to context")

	readId, ok := FromContext(ctx)
	require.True(t, ok, "getting id from context")
	require.True(t, readId == id, "expected %q but got %q", id, readId)

	// Test overwriting the id
	id = shortuuid.New()
	ctx, ok = AddToIncomingContext(ctx, id)
	require.True(t, ok, "overwriting id")
	readId, ok = FromContext(ctx)

	require.True(t, ok, "getting overwritten id from context")
	require.True(t, readId == id, "expected new id to be %q but got %q", id, readId)
}

func TestUnaryServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		id, ok := FromContext(ctx)
		if !ok {
			t.Fatal("error getting id from context")
		}
		if id == "" {
			t.Fatalf("got the empty string as id")
		}
		return nil, nil
	}

	replace := false
	f := UnaryServerInterceptor(replace)
	_, err := f(ctx, nil, nil, handler)
	require.NoError(t, err)

	replace = true
	f = UnaryServerInterceptor(replace)
	_, err = f(ctx, nil, nil, handler)
	require.NoError(t, err)
}

func TestUnaryServerInterceptorWithExisting(t *testing.T) {
	var replace bool
	id := shortuuid.New()
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	ctx, ok := AddToIncomingContext(ctx, id)
	if !ok {
		t.Fatal("error adding id to context")
	}

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		readId, ok := FromContext(ctx)
		if !ok {
			t.Fatal("error getting id from context")
		}
		if id == "" {
			t.Fatal("got the empty string as id")
		}
		if replace && readId == id {
			t.Fatal("existing id was not replaced")
		}
		if !replace && readId != id {
			t.Fatal("existing id was replaced")
		}
		return nil, nil
	}

	replace = false
	f := UnaryServerInterceptor(replace)
	_, err := f(ctx, nil, nil, handler)
	require.NoError(t, err)

	replace = true
	f = UnaryServerInterceptor(replace)
	_, err = f(ctx, nil, nil, handler)
	require.NoError(t, err)
}

func TestStreamServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	stream := &grpc_middleware.WrappedServerStream{}
	stream.WrappedContext = ctx
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		ctx := stream.Context()
		id, ok := FromContext(ctx)
		if !ok {
			t.Fatal("error getting id from context")
		}
		if id == "" {
			t.Fatalf("got the empty string as id")
		}
		return nil
	}

	replace := false
	f := StreamServerInterceptor(replace)
	err := f(nil, stream, nil, handler)
	require.NoError(t, err)

	replace = true
	f = StreamServerInterceptor(replace)
	err = f(nil, stream, nil, handler)
	require.NoError(t, err)
}

func TestStreamServerInterceptorWithExisting(t *testing.T) {
	var replace bool
	id := shortuuid.New()
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	ctx, ok := AddToIncomingContext(ctx, id)
	if !ok {
		t.Fatal("error adding id to context")
	}
	stream := &grpc_middleware.WrappedServerStream{}
	stream.WrappedContext = ctx
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		ctx := stream.Context()
		readId, ok := FromContext(ctx)
		if !ok {
			t.Fatal("error getting id from context")
		}
		if id == "" {
			t.Fatal("got the empty string as id")
		}
		if replace && readId == id {
			t.Fatal("existing id was not replaced")
		}
		if !replace && readId != id {
			t.Fatal("existing id was replaced")
		}
		return nil
	}

	replace = false
	f := StreamServerInterceptor(replace)
	err := f(nil, stream, nil, handler)
	require.NoError(t, err)

	replace = true
	f = StreamServerInterceptor(replace)
	err = f(nil, stream, nil, handler)
	require.NoError(t, err)
}
