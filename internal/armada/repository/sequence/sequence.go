package sequence

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ExternalSeqNo is a sequence number that we pass to end users
// Sequence is the Redis message sequence
// Index is the index of the event inside the armadaevents.EventSequence
// Last im
type ExternalSeqNo struct {
	Time   int64
	Seq    int64
	SubSeq int
	Last   bool
}

func Min() *ExternalSeqNo {
	return &ExternalSeqNo{0, 0, 0, true}
}

func Max() *ExternalSeqNo {
	return &ExternalSeqNo{math.MaxInt64, math.MaxInt64, math.MaxInt, true}
}

// Parse parses an external sequence number which should be of the form "Time:Seq.SubSeq:last".
// The empty string will be interpreted as "-1:-1" which is the initial sequence number
// An error will be returned if the sequence number cannot be parsed
func Parse(str string) (*ExternalSeqNo, error) {
	if str == "" {
		return &ExternalSeqNo{0, 0, 0, true}, nil
	}
	toks := strings.Split(str, "-")
	if len(toks) != 4 {
		return nil, fmt.Errorf("%s is not a valid sequence number", str)
	}
	time, err := strconv.ParseInt(toks[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid sequence number", str)
	}
	seq, err := strconv.ParseInt(toks[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid sequence number", str)
	}
	subSeq, err := strconv.Atoi(toks[2])
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid sequence number", str)
	}

	last, err := strconv.ParseBool(toks[3])
	return &ExternalSeqNo{
		Time:   time,
		Seq:    seq,
		SubSeq: subSeq,
		Last:   last,
	}, nil
}

func FromRedisId(redisId string, subSeq int, last bool) (*ExternalSeqNo, error) {
	toks := strings.Split(redisId, "-")
	if len(toks) != 2 {
		return nil, fmt.Errorf("%s is not a valid redis identifier", toks)
	}
	time, err := strconv.ParseInt(toks[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid redis identifier", toks)
	}
	seq, err := strconv.ParseInt(toks[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid redis identifier", toks)
	}
	return &ExternalSeqNo{
		Time:   time,
		Seq:    seq,
		SubSeq: subSeq,
		Last:   last,
	}, nil
}

// IsValid Returns true if the given string is a valid ExternalSeqNo
func IsValid(str string) bool {
	_, err := Parse(str)
	return err == nil
}

func (e *ExternalSeqNo) ToString() string {
	return fmt.Sprintf("%d-%d-%d-%d", e.Time, e.Seq, e.SubSeq, boolToInt(e.Last))
}

func (e *ExternalSeqNo) PrevRedisId() string {
	var seq *ExternalSeqNo
	if !e.Last && e.Time != 0 {
		seq = &ExternalSeqNo{e.Time - 1, math.MaxInt64, 0, true}
	} else {
		seq = e
	}
	return seq.ToRedisString()
}

func (e *ExternalSeqNo) ToRedisString() string {
	return fmt.Sprintf("%d-%d", e.Time, e.Seq)
}

func (e *ExternalSeqNo) IsAfter(other *ExternalSeqNo) bool {
	if other == nil {
		return false
	}
	if e.Time > other.Time {
		return true
	}
	if e.Time == other.Time && e.Seq > other.Seq {
		return true
	}

	if e.Time == other.Time && e.Seq == other.Seq && e.SubSeq > other.SubSeq {
		return true
	}

	return false
}

func boolToInt(b bool) int8 {
	if b {
		return 1
	} else {
		return 0
	}
}
