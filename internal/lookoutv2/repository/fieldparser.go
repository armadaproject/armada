package repository

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
	"github.com/jackc/pgtype"
	"github.com/pkg/errors"
	"math"
	"time"
)

type FieldParser interface {
	GetField() string
	GetVariableRef() interface{}
	ParseValue() (interface{}, error)
}

type LastTransitionTimeParser struct {
	variable pgtype.Numeric
}

func (fp *LastTransitionTimeParser) GetField() string {
	return lastTransitionTimeField
}

func (fp *LastTransitionTimeParser) GetVariableRef() interface{} {
	return &fp.variable
}

func (fp *LastTransitionTimeParser) ParseValue() (interface{}, error) {
	var dst float64
	err := fp.variable.AssignTo(&dst)
	if err != nil {
		return "", err
	}
	t := time.Unix(int64(math.Round(dst)), 0)
	return t.Format(time.RFC3339), nil
}

type TimeParser struct {
	field    string
	variable time.Time
}

func (fp *TimeParser) GetField() string {
	return fp.field
}

func (fp *TimeParser) GetVariableRef() interface{} {
	return &fp.variable
}

func (fp *TimeParser) ParseValue() (interface{}, error) {
	return fp.variable.Format(time.RFC3339), nil
}

type StateParser struct {
	variable int16
}

func (fp *StateParser) GetField() string {
	return stateField
}

func (fp *StateParser) GetVariableRef() interface{} {
	return &fp.variable
}

func (fp *StateParser) ParseValue() (interface{}, error) {
	state, ok := lookout.JobStateMap[int(fp.variable)]
	if !ok {
		return "", errors.Errorf("state not found: %d", fp.variable)
	}
	return string(state), nil
}

type BasicParser[T any] struct {
	field    string
	variable T
}

func (fp *BasicParser[T]) GetField() string {
	return fp.field
}

func (fp *BasicParser[T]) GetVariableRef() interface{} {
	return &fp.variable
}

func (fp *BasicParser[T]) ParseValue() (interface{}, error) {
	return fp.variable, nil
}

func ParserForGroup(field string) FieldParser {
	switch field {
	case stateField:
		return &StateParser{}
	default:
		return &BasicParser[string]{field: field}
	}
}

func ParsersForAggregate(field string, filters []*model.Filter) ([]FieldParser, error) {
	var parsers []FieldParser
	switch field {
	case lastTransitionTimeField:
		parsers = append(parsers, &LastTransitionTimeParser{})
	case submittedField:
		parsers = append(parsers, &TimeParser{field: submittedField})
	case stateField:
		states := GetStatesForFilter(filters)
		for _, state := range states {
			parsers = append(parsers, &BasicParser[int]{field: fmt.Sprintf("%s%s", stateAggregatePrefix, state)})
		}
	default:
		return nil, errors.Errorf("no aggregate found for field %s", field)
	}
	return parsers, nil
}
