package health

import "errors"

type StartupCompleteChecker struct {
	complete bool
}

func NewStartupCompleteChecker() *StartupCompleteChecker {
	return &StartupCompleteChecker{
		complete: false,
	}
}

func (checker *StartupCompleteChecker) Check() error {
	if checker.complete {
		return nil
	} else {
		return errors.New("Startup not complete yet.")
	}
}

func (checker *StartupCompleteChecker) MarkComplete() {
	checker.complete = true
}
