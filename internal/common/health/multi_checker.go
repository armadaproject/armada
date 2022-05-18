package health

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// MultiChecker combines multiple Checker instances.
// Calls to MultiChecker.Check() calls Check() on each constituent checker
// and returns a new error created by joining any errors returned from those calls,
// or nil if no errors are found.
type MultiChecker struct {
	mu       sync.Mutex
	checkers []Checker
}

func NewMultiChecker(checkers ...Checker) *MultiChecker {
	return &MultiChecker{
		checkers: checkers,
	}
}

func (mc *MultiChecker) Check() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if len(mc.checkers) == 0 {
		return fmt.Errorf("no checkers registered")
	}

	errorStrings := []string{}
	for _, checker := range mc.checkers {
		err := checker.Check()
		if err != nil {
			errorStrings = append(errorStrings, err.Error())
		}
	}

	if len(errorStrings) == 0 {
		return nil
	} else {
		return errors.New(strings.Join(errorStrings, "\n"))
	}
}

func (mc *MultiChecker) Add(checker Checker) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.checkers = append(mc.checkers, checker)
}
