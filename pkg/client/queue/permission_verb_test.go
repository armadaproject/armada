package queue

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

var permissionVerbsToTest = []PermissionVerb{
	PermissionVerbCancel,
	PermissionVerbReprioritize,
	PermissionVerbPreempt,
	PermissionVerbSubmit,
	PermissionVerbWatch,
	"custom_verb_1",
	"cust-vrb-2",
}

func TestPermissionVerbUnmarshal(t *testing.T) {
	for _, verb := range permissionVerbsToTest {
		data, err := json.Marshal(verb)
		if err != nil {
			t.Errorf("failed to marshal verb: %s to json: %s", verb, err)
		}
		result := PermissionVerb("")

		err = json.Unmarshal(data, &result)
		require.NoErrorf(t, err, "failed to unmarshal valid verb %s", verb)

	}
}
