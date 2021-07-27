package health

import (
	"errors"
	"strings"
)

type MultiChecker struct {
	checkers []Checker
}

func NewMultiChecker(checkers ...Checker) *MultiChecker {
	return &MultiChecker{
		checkers: checkers,
	}
}

func (mc *MultiChecker) Check() error {
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
	mc.checkers = append(mc.checkers, checker)
}
