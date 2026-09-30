package configuration

import (
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArmadaConfig_Validate_ObjectNamePrefix(t *testing.T) {
	tests := []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		{name: "the default prefix is valid", prefix: "armada"},
		{name: "a prefix of 20 characters is valid", prefix: "abcdefghij-abcdefghi"},
		{name: "a prefix of 21 characters is not valid", prefix: "abcdefghij-abcdefghij", wantErr: true},
		{name: "an empty prefix is not valid", prefix: "", wantErr: true},
		{name: "a prefix with an upper case letter is not valid", prefix: "Armada", wantErr: true},
		{name: "a prefix that starts with a digit is not valid", prefix: "1armada", wantErr: true},
		{name: "a prefix that ends with a dash is not valid", prefix: "armada-", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ArmadaConfig{Submission: SubmissionConfig{ObjectNamePrefix: tc.prefix}}.Validate()
			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			var validationErrors validator.ValidationErrors
			require.ErrorAs(t, err, &validationErrors)
			require.Len(t, validationErrors, 1)
			assert.Equal(t, "ObjectNamePrefix", validationErrors[0].Field())
			assert.Equal(t, objectNamePrefixErrorMessage, validationErrors[0].Tag())
		})
	}
}
