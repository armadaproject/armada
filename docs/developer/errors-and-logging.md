# Creating, wrapping and logging errors

How you create and pass an error decides two things:

- **What operators can debug.** When an error is logged, the line can include a `stacktrace` field showing where the error started. If the error has no stack, or the chain back to the stack is broken, the log only says what went wrong, not where.
- **What API users see.** Errors returned from gRPC handlers are turned into a gRPC status code and message. If the error type is lost along the way, a client gets `Unknown` for something that should have been `NotFound` or `InvalidArgument`.

This guide covers which error functions to use and when.

## Quick reference

| You're... | Do this |
|---|---|
| Creating a new error | `errors.New` / `errors.Errorf` from pkg/errors |
| Defining a package-level sentinel | stdlib `errors.New`, then `errors.WithStack` where it's returned |
| Returning an error from a library call | `errors.WithStack(err)` or `errors.Wrap(err, "...")` |
| Adding context to one of our errors | `fmt.Errorf("...: %w", err)` or `errors.WithMessage` |
| Returning a user-facing API error | An `armadaerrors` type with a clear message |
| Logging an unexpected failure | `ctx.Logger().WithStacktrace(err).Error(...)` |
| Logging an expected or retried failure | `ctx.Logger().WithError(err).Warn(...)` |
| Including an error in `fmt.Errorf` | Always `%w` |

## What a good error log line looks like

```json
{
  "level": "error",
  "error": "fetching job updates: selecting initial jobs: context deadline exceeded",
  "stacktrace": "[job_repository.go:109 ... scheduler.go:272 ...]",
  "caller": "scheduler.go:272",
  "message": "cycle failure"
}
```

- `error` is the full message chain, outermost context first. 
- `stacktrace` points to where the error **started**. The logger walks the whole wrap chain and reports the deepest stack it finds.
- `caller` is the line that wrote the log. It's added automatically, so you don't need to do anything for it.

If `stacktrace` is missing from an error log, either no stack was ever attached, or something in the chain broke it. The rest of this guide is about avoiding both.

## Which package to use

Armada uses [`github.com/pkg/errors`](https://pkg.go.dev/github.com/pkg/errors) as well as the standard library. The difference that matters:

| Function | Captures a stack? | Use it for |
|---|---|---|
| `errors.New(msg)` (pkg/errors) | Yes | A new error raised in our code |
| `errors.Errorf(fmt, args...)` (pkg/errors) | Yes | A new error with a formatted message |
| `errors.WithStack(err)` | Yes | An error from a library, with no extra message |
| `errors.Wrap(err, msg)` / `errors.Wrapf` | Yes | An error from a library, with context |
| `errors.WithMessage(err, msg)` / `errors.WithMessagef` | No | Adding context to an error that already has a stack |
| `fmt.Errorf("...: %w", err)` | No | Adding context to an error that already has a stack |
| `fmt.Errorf("...: %s", err)` or `%v` | No, and it **breaks the chain** | Never use this to wrap an error |
| stdlib `errors.New(msg)` | No | Package-level sentinel errors (see below) |

## Creating a new error

If you're raising an error in our own code, use pkg/errors so that the error records where it was created:

```go
// Good
return errors.Errorf("queue context for queue %s not found", queue)

// Bad: no stack, so the log can't show where this came from
return fmt.Errorf("queue context for queue %s not found", queue)
```

**Exception: sentinel errors.** Package-level errors should use stdlib `errors.New`. A pkg/errors sentinel captures the stack from package initialisation, which tells you nothing. Add the stack where the sentinel is returned instead:

```go
var ErrLeaseExpired = stderrors.New("lease expired")

func (l *Lease) Renew() error {
    if l.expired() {
        return errors.WithStack(ErrLeaseExpired)
    }
    ...
}
```

## Errors from libraries and external systems

Errors from pgx, Pulsar, gRPC clients, protobuf, the Kubernetes client and most other libraries have **no stack trace**. Wrapping them with `fmt.Errorf` doesn't add one. If you pass them up without adding a stack, every log line for that failure has an `error` field and no `stacktrace`.

Add the stack **where the library error first enters Armada code**:

```go
rows, err := tx.Query(ctx, query)
if err != nil {
    return errors.WithStack(err)
}

jobs, err := queries.SelectInitialJobs(ctx, params)
if err != nil {
    return errors.Wrap(err, "selecting initial jobs")
}
```

Some practical points:

- `errors.WithStack(nil)` and `errors.Wrap(nil, ...)` return `nil`, so `return result, errors.WithStack(err)` is safe without an `if err != nil` check.
- Adding a stack higher up the call path does no harm, because the logger prefers the deepest one. But it doesn't fix a missing stack lower down. 
- Closures passed to helpers such as transaction or retry wrappers are boundaries too. Add the stack inside the closure, or on the helper's return value.

## Adding context as an error moves up

Each layer can add a short description of what it was doing. This builds the readable `error` field (`fetching job updates: selecting initial jobs: ...`).

- If the error **already has a stack** (it came from our code or was wrapped at a boundary), use `fmt.Errorf("...: %w", err)` or `errors.WithMessage(err, "...")`. They're equivalent for logging purposes.
- If you **aren't sure** whether it has a stack, `errors.Wrap` is always safe.

**Always use `%w`, never `%s` or `%v`, to include an error in `fmt.Errorf`.**

```go
// Good
return fmt.Errorf("failed fetching executors from db: %w", err)

// Bad: flattens the error to a string
return fmt.Errorf("failed fetching executors from db - %s", err)
```

`%s` turns the inner error into plain text. Its message survives, but everything else is lost:

- the stack trace,
- `errors.Is` / `errors.As`, so retry logic, `pgx.ErrNoRows` checks and similar no longer match
- the `armadaerrors` type, so an API user gets `Unknown` instead of the right gRPC code

Note that the stack lookup has two limits:

- It only goes 10 layers deep. In practice this is plenty, but don't wrap the same error more than once per function.
- It only follows errors that wrap a single error. A stack inside `errors.Join`, or inside `fmt.Errorf` with more than one `%w`, isn't found. If you combine errors this way, a missing `stacktrace` doesn't mean no stack was recorded.

## Errors returned to API users

Code that handles gRPC requests should return the types in [`internal/common/armadaerrors`](../../internal/common/armadaerrors/errors.go) for failures that are the caller's problem. The gRPC interceptor finds them anywhere in the chain and sets the status code:

| Situation | Return | Client sees |
|---|---|---|
| Bad input in the request | `&armadaerrors.ErrInvalidArgument{Name: "priorityFactor", Value: factor, Message: "must be positive"}` | `InvalidArgument` |
| Requested object doesn't exist | `&armadaerrors.ErrNotFound{Type: "queue", Value: queueName}` | `NotFound` |
| Object already exists | `&armadaerrors.ErrAlreadyExists{Type: "queue", Value: queueName}` | `AlreadyExists` |
| Job can't fit on any node | `*armadaerrors.ErrPodUnschedulable` | `InvalidArgument` |
| Anything else | A normal wrapped error | `Unknown` |

When you write these:

- **The message goes to the user.** Write it for someone submitting jobs, not someone reading the scheduler source. Say what was wrong and, where possible, how to fix it. Don't include internal hostnames, SQL, or stack details.
- **You can wrap them**, as long as you use `%w`, `errors.Wrap` or `errors.WithMessage`. The full chain becomes the message, and the type still sets the code.
- **Messages are truncated** to the configured maximum size, so put the important part first.
- **They don't carry a stack.** That's usually fine, since user errors are expected and don't need a trace. If you do want one for a server-side log, wrap the error with `errors.WithStack`.

If you're returning an error that isn't the user's fault (for example, the database being down), don't dress it up as `InvalidArgument`. Let it come through as `Unknown`, and keep two things in mind:

- **Log it in the handler with `WithStacktrace`.** The gRPC request log runs after the error has been converted to a status, so it never sees the original stack. Nothing else will log it with a stack for you.
- **Its full message is still sent to the client.** Every wrap message in the chain is included, so don't put anything in them that a user shouldn't see.

## Logging errors

### `WithError` or `WithStacktrace`

Both put the error in the `error` field. `WithStacktrace` also adds the `stacktrace` field when the chain has one.

Use **`WithStacktrace`** when the error is unexpected and someone will need to find out where it came from:

- cycle failures and failed background loops
- anything at `Error` level that points to a bug or an infrastructure problem

For a recovered panic, capture and log `debug.Stack()` in the recovery handler. `WithStacktrace` only reads a stack already stored on an error, and a recovered panic value usually doesn't have one.

```go
ctx.Logger().WithStacktrace(err).Error("scheduling cycle failed")
```

Use **`WithError`** when the error is expected or already understood, and a trace would just be noise:

- user errors, validation failures, rejected requests
- transient failures you're about to retry, usually at `Warn` level
- shutdown and context-cancellation errors

```go
ctx.Logger().WithError(err).Warn("failed to publish markers, will retry")
```

### Guidelines

- **Log the error you actually have.** Make sure the value passed to `WithStacktrace` / `WithError` is the real error. It's easy to pass a stale or nil `err` from an earlier call when the real failure is in something like `result.Error()`, and the log then has no stack.
- **Put the error in the field, not the message.** Prefer `WithError(err).Errorf("scheduling on pool %s failed", pool)` over `Errorf("... error %s", pool, err)`. The field can be searched and filtered in log tooling, and only the field gets the stack.
- **Log once, where the error is handled.** If you return the error, don't also log it. Logging at every layer produces several lines for one failure, each with less context than the last. The place that decides what to do (retry, skip, abort, return to the user) is the place to log.
- **Use the context logger.** `ctx.Logger()` and `ctx.Errorf` include the fields attached to the `armadacontext.Context`, such as cycle number, job ID or queue, which helps tie a failure to what it was working on.
- **Don't add `caller` yourself.** It's added once per line automatically. If you write a helper that wraps the logger, call `logger.WithCallerSkip(1)` inside it so that `caller` points at the helper's caller and not at the helper.
