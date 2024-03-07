package metrics

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFoo(t *testing.T) {
	r, err := regexp.Compile("foo.*bar")
	require.NoError(t, err)
	assert.True(t, r.MatchString("foobar"))
	assert.True(t, r.MatchString("foo bar"))
	assert.True(t, r.MatchString("foo and bar"))
	assert.True(t, r.MatchString("this is foo and bar so"))
	assert.False(t, r.MatchString("barfoo"))
	assert.False(t, r.MatchString("foo"))
	assert.False(t, r.MatchString("bar"))
}
