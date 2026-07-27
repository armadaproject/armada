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

// UnmarshalJSON for RetryAction accepts the short action name ("Fail",
// "Retry", case-insensitive), the canonical proto name ("RETRY_ACTION_FAIL"),
// or a numeric value, so operator-authored YAML can use the natural short form
// rather than the prefixed proto enum name.
func (x *RetryAction) UnmarshalJSON(data []byte) error {
	var n int32
	if err := json.Unmarshal(data, &n); err == nil {
		if _, ok := RetryAction_name[n]; !ok {
			return fmt.Errorf("no RetryAction of value %d", n)
		}
		*x = RetryAction(n)
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if v, ok := RetryAction_value[s]; ok {
		*x = RetryAction(v)
		return nil
	}
	if v, ok := RetryAction_value["RETRY_ACTION_"+strings.ToUpper(s)]; ok {
		*x = RetryAction(v)
		return nil
	}
	return fmt.Errorf("no RetryAction of name %q", s)
}

// MarshalJSON emits the short action name ("Fail"/"Retry") rather than the raw
// enum integer, so marshalled policies read naturally and round-trip back
// through UnmarshalJSON. Value receiver so it applies to both RetryAction and
// *RetryAction.
func (x RetryAction) MarshalJSON() ([]byte, error) {
	canonical, ok := RetryAction_name[int32(x)]
	if !ok {
		return nil, fmt.Errorf("no RetryAction of value %d", int32(x))
	}
	name := strings.TrimPrefix(canonical, "RETRY_ACTION_")
	return json.Marshal(strings.ToUpper(name[:1]) + strings.ToLower(name[1:]))
}
