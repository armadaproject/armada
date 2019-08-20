package cmd

import (
	"context"
	"time"
)

func timeout() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	return ctx
}
