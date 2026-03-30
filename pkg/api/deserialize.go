package api

import (
	"encoding/json"
	"fmt"
	"strings"
)

func (x *IngressType) UnmarshalJSON(data []byte) error {
	var s int32
	e := json.Unmarshal(data, &s)
	if e == nil {
		_, present := IngressType_name[s]
		if !present {
			return fmt.Errorf("no IngressType of type %d", s)
		}
		*x = IngressType(s)
		return nil
	}
	var t string
	e = json.Unmarshal(data, &t)
	if e != nil {
		return e
	}
	value, present := IngressType_value[t]
	if !present {
		return fmt.Errorf("no IngressType of type %s", t)
	}
	*x = IngressType(value)
	return nil
}

func (x *ServiceType) UnmarshalJSON(data []byte) error {
	var s int32
	e := json.Unmarshal(data, &s)
	if e == nil {
		_, present := ServiceType_name[s]
		if !present {
			return fmt.Errorf("no ServiceType of type %d", s)
		}
		*x = ServiceType(s)
		return nil
	}
	var t string
	e = json.Unmarshal(data, &t)
	if e != nil {
		return e
	}
	value, present := ServiceType_value[t]
	if !present {
		return fmt.Errorf("no ServiceType of type %s", t)
	}
	*x = ServiceType(value)
	return nil
}

func (x *JobState) UnmarshalJSON(data []byte) error {
	var s int32
	e := json.Unmarshal(data, &s)
	if e == nil {

		_, present := JobState_name[s]
		if !present {
			return fmt.Errorf("no JobState of type %d", s)
		}
		*x = JobState(s)
		return nil
	}
	var t string
	e = json.Unmarshal(data, &t)
	if e != nil {
		return e
	}
	value, present := JobState_value[t]
	if !present {
		return fmt.Errorf("no JobState of type %s", t)
	}
	*x = JobState(value)
	return nil
}

// UnmarshalJSON for RetryAction accepts the proto canonical name
// ("RETRY_ACTION_FAIL"), a numeric value, or a friendly alias ("Fail", "Retry"),
// so operator-authored YAML can use the natural short form rather than the
// prefixed proto enum name.
func (x *RetryAction) UnmarshalJSON(data []byte) error {
	v, err := unmarshalProtoEnum(data, "RetryAction", "RETRY_ACTION_", RetryAction_value, RetryAction_name)
	if err != nil {
		return err
	}
	*x = RetryAction(v)
	return nil
}

// UnmarshalJSON for ExitCodeOperator accepts the proto canonical name
// ("EXIT_CODE_OPERATOR_IN"), a numeric value, or a friendly alias ("In", "NotIn").
func (x *ExitCodeOperator) UnmarshalJSON(data []byte) error {
	v, err := unmarshalProtoEnum(data, "ExitCodeOperator", "EXIT_CODE_OPERATOR_", ExitCodeOperator_value, ExitCodeOperator_name)
	if err != nil {
		return err
	}
	*x = ExitCodeOperator(v)
	return nil
}

// unmarshalProtoEnum decodes a proto3 enum from JSON, accepting:
//   - a numeric value (e.g. 1)
//   - the canonical proto name (e.g. "RETRY_ACTION_FAIL")
//   - a friendly alias derived from the canonical name with prefix stripped
//     and case/underscores ignored (e.g. "Fail", "fail", "NotIn", "not_in").
//
// typeName is used only for error messages. canonicalPrefix identifies the
// portion of every canonical name to strip when matching friendly aliases
// (e.g. "RETRY_ACTION_").
func unmarshalProtoEnum(
	data []byte,
	typeName, canonicalPrefix string,
	byName map[string]int32,
	byValue map[int32]string,
) (int32, error) {
	var n int32
	if err := json.Unmarshal(data, &n); err == nil {
		if _, ok := byValue[n]; !ok {
			return 0, fmt.Errorf("no %s of value %d", typeName, n)
		}
		return n, nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return 0, err
	}
	if v, ok := byName[s]; ok {
		return v, nil
	}
	want := normalizeAlias(s)
	for canonical, v := range byName {
		if normalizeAlias(strings.TrimPrefix(canonical, canonicalPrefix)) == want {
			return v, nil
		}
	}
	return 0, fmt.Errorf("no %s of name %q", typeName, s)
}

// normalizeAlias collapses an enum-name candidate to a comparable form by
// stripping underscores and lower-casing, so "NotIn"/"NOT_IN"/"not_in" all
// canonicalise to "notin".
func normalizeAlias(s string) string {
	return strings.ToLower(strings.ReplaceAll(s, "_", ""))
}
