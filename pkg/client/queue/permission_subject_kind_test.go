package queue

import (
	"encoding/json"
	"testing"
)

func TestPermissionSubjectKindUnmarshal(t *testing.T) {
	tests := map[string]struct {
		Verbs []PermissionSubjectKind
		Fail  bool
	}{
		"ValidKind": {
			Verbs: []PermissionSubjectKind{
				PermissionSubjectKindUser,
				PermissionSubjectKindGroup,
			},
			Fail: false,
		},
		"InvalidKind": {
			Verbs: []PermissionSubjectKind{
				"",
				"random_kind1",
				"random_kind2",
			},
			Fail: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(subT *testing.T) {
			for _, verb := range test.Verbs {
				data, err := json.Marshal(verb)
				if err != nil {
					t.Errorf("failed to marshal subject kind %s to json: %s", verb, err)
				}
				result := PermissionSubjectKind("")

				err = json.Unmarshal(data, &result)
				if test.Fail && err == nil {
					t.Fatalf("failed to throw an error on invalid subject kind string %s", err)
				}
				if !test.Fail && err != nil {
					t.Fatalf("failed to unmarshal valid subject kind %s. %s", verb, err)
				}
			}
		})
	}
}
