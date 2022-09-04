package sequence

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	validIds := map[string]*ExternalSeqNo{
		"1:2:3:0": {
			Time:   1,
			Seq:    2,
			SubSeq: 3,
			Last:   false,
		},
		"1:2:3:1": {
			Time:   1,
			Seq:    2,
			SubSeq: 3,
			Last:   true,
		},
		"55555:666666:777777:1": {
			Time:   55555,
			Seq:    666666,
			SubSeq: 777777,
			Last:   true,
		},
		"": {
			Time:   0,
			Seq:    0,
			SubSeq: 0,
			Last:   true,
		},
	}

	for k, v := range validIds {
		id, err := Parse(k)
		assert.NoError(t, err)
		assert.Equal(t, v, id)
		assert.True(t, IsValid(k))
	}

	invalidIds := []string{"foo", "1", "1:1:1", ":::"}
	for _, k := range invalidIds {
		_, err := Parse(k)
		assert.Error(t, err)
		assert.False(t, IsValid(k))
	}
}

func TestToRedisString(t *testing.T) {
	ids := map[ExternalSeqNo]string{
		{
			Time:   1,
			Seq:    2,
			SubSeq: 3,
			Last:   false,
		}: "1-2",
		{
			Time:   1111,
			Seq:    2222,
			SubSeq: 3333,
			Last:   true,
		}: "1111-2222",
	}

	for k, v := range ids {
		assert.Equal(t, v, k.RedisString())
	}
}

func TestToString(t *testing.T) {
	ids := map[string]*ExternalSeqNo{
		"1:2:3:0": {
			Time:   1,
			Seq:    2,
			SubSeq: 3,
			Last:   false,
		},
		"1:2:3:1": {
			Time:   1,
			Seq:    2,
			SubSeq: 3,
			Last:   true,
		},
		"55555:666666:777777:1": {
			Time:   55555,
			Seq:    666666,
			SubSeq: 777777,
			Last:   true,
		},
		"0:0:0:1": {
			Time:   0,
			Seq:    0,
			SubSeq: 0,
			Last:   true,
		},
	}

	for k, v := range ids {
		assert.Equal(t, k, v.String())
	}
}

func TestFromRedisId(t *testing.T) {
	expected := &ExternalSeqNo{
		Time:   1,
		Seq:    2,
		SubSeq: 3,
		Last:   false,
	}

	// last = false
	id, err := FromRedisId("1-2", 3, false)
	assert.NoError(t, err)
	assert.Equal(t, expected, id)

	// last = true
	expected.Last = true
	id, err = FromRedisId("1-2", 3, true)
	assert.NoError(t, err)
	assert.Equal(t, expected, id)

	// invalid
	_, err = FromRedisId("foo", 3, true)
	assert.Error(t, err)
}

func TestPrevRedisId(t *testing.T) {
	id := &ExternalSeqNo{
		Time:   1,
		Seq:    2,
		SubSeq: 3,
		Last:   false,
	}

	// not last
	assert.Equal(t, "1-1", id.PrevRedisId())

	// not last
	id.Last = true
	assert.Equal(t, "1-2", id.PrevRedisId())

	id = &ExternalSeqNo{
		Time:   1,
		Seq:    0,
		SubSeq: 0,
		Last:   false,
	}

	// not last and we need to go back in time
	assert.Equal(t, "0-9223372036854775807", id.PrevRedisId())
}

func TestIsAfter(t *testing.T) {
	ids := []*ExternalSeqNo{
		{
			Time:   0,
			Seq:    0,
			SubSeq: 0,
		},
		{
			Time:   0,
			Seq:    1,
			SubSeq: 0,
		},
		{
			Time:   0,
			Seq:    1,
			SubSeq: 1,
		},
		{
			Time:   1,
			Seq:    0,
			SubSeq: 0,
		},
	}

	for i := 0; i < len(ids)-1; i++ {
		a := ids[i]
		b := ids[i+1]
		assert.False(t, a.IsAfter(b))
		assert.True(t, b.IsAfter(a))
	}
}
