package queue

import (
	"encoding/json"
	"testing"
)

func TestPermissionVerbUnmarshal(t *testing.T) {
	tests := map[string]struct {
		Verbs []PermissionVerb
		Fail  bool
	}{
		"ValidVerbs": {
			Verbs: []PermissionVerb{
				PermissionVerbCancel,
				PermissionVerbReprioritize,
				PermissionVerbSubmit,
				PermissionVerbWatch,
			},
			Fail: false,
		},
		"InvalidVerbs": {
			Verbs: []PermissionVerb{"", "random_verb1", "random_verb2"},
			Fail:  true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(subT *testing.T) {
			for _, verb := range test.Verbs {
				data, err := json.Marshal(verb)
				if err != nil {
					t.Errorf("failed to marshal verb: %s to json: %s", verb, err)
				}
				result := PermissionVerb("")

				err = json.Unmarshal(data, &result)
				if test.Fail && err == nil {
					t.Fatalf("failed to throw an error on invalid verb string %s", err)
				}
				if !test.Fail && err != nil {
					t.Fatalf("failed to unmarshal valid verb %s. %s", verb, err)
				}

			}
		})
	}
}
