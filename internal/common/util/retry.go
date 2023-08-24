package util

import "golang.org/x/net/context"

func RetryUntilSuccess(ctx context.Context, performAction func() error, onError func(error)) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := performAction()
			if err == nil {
				return
			} else {
				onError(err)
			}
		}
	}
}
