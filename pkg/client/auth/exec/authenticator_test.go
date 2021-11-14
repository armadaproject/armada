package exec

import "testing"
import "github.com/stretchr/testify/assert"

func TestAuthenticatorHappyPath(t *testing.T) {

	a := Authenticator{
		cmd:         "./testdata/test-exec.sh",
		env:         []string{"EXEC_TEST_OUTPUT=aToken", "EXEC_TEST_EXIT_CODE=0"},
		interactive: false,
		environ:     func() []string { return nil },
	}

	tok, err := a.GetCreds()

	assert.Empty(t, err)
	assert.Equal(t, "aToken", tok)
}

func TestAuthenticatorCmdFails(t *testing.T) {

	a := Authenticator{
		cmd:         "./testdata/test-exec.sh",
		env:         []string{"EXEC_TEST_OUTPUT=aToken", "EXEC_TEST_EXIT_CODE=1"},
		interactive: false,
		environ:     func() []string { return nil },
	}

	tok, err := a.GetCreds()

	assert.NotEmpty(t, err)
	assert.Empty(t, tok)
}

func TestAuthenticatorMissingCmd(t *testing.T) {

	a := Authenticator{
		cmd:         "no_a_valid_command.sh",
		env:         []string{"EXEC_TEST_OUTPUT=aToken", "EXEC_TEST_EXIT_CODE=0"},
		interactive: false,
		environ:     func() []string { return nil },
	}

	tok, err := a.GetCreds()

	assert.NotEmpty(t, err)
	assert.Empty(t, tok)
}

func TestAuthenticatorNoToken(t *testing.T) {

	a := Authenticator{
		cmd:         "./testdata/test-exec.sh",
		env:         []string{"EXEC_TEST_OUTPUT=", "EXEC_TEST_EXIT_CODE=0"},
		interactive: false,
		environ:     func() []string { return nil },
	}

	tok, err := a.GetCreds()

	assert.NotEmpty(t, err)
	assert.Empty(t, tok)
}
