package logging

// TopmostWithCause recursively calls cause on the given error until it finds an error that does
// not implement the causer interface, and returns the error directly preceding that one.
// Typically, that is the final or penultimate error in the chain.
// This function is meant to be used together with pkg/errors wrapping.
//
// Logging the error returned by this one with the %+v verb provides a stack trace recorded at
// the point the error was created.
func TopmostWithCause(err error) error {
	type causer interface {
		Cause() error
	}

	rv := err
	for rv != nil {
		cause, ok := rv.(causer)
		if !ok {
			break
		}
		err = cause.Cause()
		_, ok = err.(causer)
		if !ok {
			break
		}
		rv = err
	}
	return rv
}
