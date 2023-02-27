package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTabWriter_TestSimple(t *testing.T) {
	w := NewTabWriter(1, 1, 1, ' ', 0)
	w.Writef("a:\t%s", "b")
	assert.Equal(t, "a: b", w.String())
}

func TestTabWriter_TestComplex(t *testing.T) {
	w := NewTabWriter(1, 1, 1, ' ', 0)
	w.Writef("a:\t%s\n", "b")
	w.Writef("a:\t%.2f\t%d\n", 1.5, 2)
	assert.Equal(t, "a: b\na: 1.50 2\n", w.String())
}
