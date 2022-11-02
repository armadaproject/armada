package api

import (
	"encoding/json"
	"fmt"
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
