package api

import (
	"encoding/json"
	"fmt"
)

func (x *IngressType) UnmarshalJSON(data []byte) error {
	var s int32
	e := json.Unmarshal(data, &s)
	if e == nil {
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
