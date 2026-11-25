# Creating, wrapping and logging errors

How you create and pass an error decides two things:

- **What operators can debug.** When an error is logged, the line can include a `stacktrace` field showing where the error started. If the error has no stack, or the chain back to the stack is broken, the log only says what went wrong, not where.
- **What API users see.** Errors returned from gRPC handlers are turned into a gRPC status code and message. If the error type is lost along the way, a client gets `Unknown` for something that should have been `NotFound` or `InvalidArgument`.

## Best Practices

To keep error logging consistent, this is what we do in the project. `errors` means [`github.com/pkg/errors`](https://pkg.go.dev/github.com/pkg/errors).

- Use `errors.New` / `errors.Errorf` to create new errors, not `fmt.Errorf`. This records where the error was created.
- Use the standard library `errors.New` for package-level sentinel errors, and `errors.WithStack(ErrX)` where they're returned. 
- Use `errors.Wrap` for all errors from the standard library or external libraries (pgx, Pulsar, gRPC, Kubernetes, ...). This will add a stack trace where the error enters the applications.
- Use `errors.WithMessage` to add context to an error that's already been wrapped.
- Wrap joined errors with `errors.WithStack(errors.Join(errs...))`. The logger doesn't look inside joined errors, so without this the log has no stack at all.
- When adding error context with `fmt.Errorf()` always use `%w`.Never include an error with `%s` or `%v`. It turns the error into plain text, which loses the stack trace, breaks `errors.Is` / `errors.As`, and loses the gRPC status code.
- Use the [`armadaerrors`](../../internal/common/armadaerrors/errors.go) types (`ErrInvalidArgument`, `ErrNotFound`, `ErrAlreadyExists`) for all user-facing errors. The gRPC interceptor turns them into the matching status code. Anything else reaches the client as `Unknown`.
- Use `WARN` level for expected or retried errors: `ctx.Logger().WithError(err).Warn(...)`.
- Use `ERROR` level for unexpected errors: `ctx.Logger().WithStacktrace(err).Error(...)`.
- Log an error once, where it's handled, not at every layer it passes through. gRPC handlers are the exception: log unexpected errors there with `WithStacktrace` as well as returning them, because the request log never sees the stack.

## What makes a good error log

```json
{
  "level": "error",
  "error": "fetching job updates: selecting initial jobs: context deadline exceeded",
  "stacktrace": "[job_repository.go:109 ... scheduler.go:272 ...]",
  "caller": "scheduler.go:272",
  "message": "cycle failure"
}
```

- **`error`** is the full message chain, outermost context first. Each wrap should add a short description of what that layer was doing, so the chain reads as one sentence.
- **`stacktrace`** points to where the error started. If it's missing, either no stack was ever attached or something in the chain broke it, usually `fmt.Errorf` instead of `errors.Wrap`, or `%s` instead of `%w`.
- **`caller`** is the line that wrote the log. It's added automatically.
- **`message`** says what failed from the point of view of the code that logged it. Keep the error itself in the `error` field (`WithError(err).Errorf("scheduling on pool %s failed", pool)`), not in the message, so it can be searched and filtered.

For user-facing errors, the message is sent to the client. Write it for someone submitting jobs: say what was wrong and how to fix it, and leave out internal hostnames, SQL and stack details. Every wrap message in the chain is included, and long messages are truncated, so put the important part first.
