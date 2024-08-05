package sequence

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ExternalSeqNo is a sequence number that we pass to end users
type ExternalSeqNo struct {
	Time   int64 // Redis assigned time
	Seq    int64 // Redis assigned sequence
	SubSeq int   // Index of message in our eventSequence
	Last   bool  // flag to indicate if this is the las message in the subsequence
}

// Parse parses an external sequence number which should be of the form "Time:Seq:SubSeq:last".
// The empty string will be interpreted as "0:0:0" which is the initial sequence number
// An error will be returned if the sequence number cannot be parsed
func Parse(str string) (*ExternalSeqNo, error) {
	if str == "" {
		return &ExternalSeqNo{0, 0, 0, true}, nil
	}
	toks := strings.Split(str, ":")
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
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid sequence number", str)
	}
	return &ExternalSeqNo{
		Time:   time,
		Seq:    seq,
		SubSeq: subSeq,
		Last:   last,
	}, nil
}

// FromRedisId creates an ExternalSeqNo from the redis string, subsequence and last index flag
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

func (e *ExternalSeqNo) String() string {
	return fmt.Sprintf("%d:%d:%d:%d", e.Time, e.Seq, e.SubSeq, boolToInt(e.Last))
}

// PrevRedisId returns the redis id that we would have to query *from* in order to guarantee that we would
// receive subsequent messages.  Note that this is somewhat complex as if this message is not the last message
// In the event sequence we need to make sure we refetch the redis message referenced in this seqNo.
// If the message is the last sequence number then the redis id is the current message
// Else if the redis SubSeq is greater than zero we need to decrement the SubSeq by one
// Else if Time is greater than zero then we need to decrement the time by one
// Else we just need to return the initial sequence
func (e *ExternalSeqNo) PrevRedisId() string {
	var seq *ExternalSeqNo
	if e.Last {
		seq = e
	} else if e.Seq > 0 && e.SubSeq > 0 {
		seq = &ExternalSeqNo{e.Time, e.Seq - 1, 0, true}
	} else if e.Time > 0 {
		seq = &ExternalSeqNo{e.Time - 1, math.MaxInt64, 0, true}
	} else {
		seq = &ExternalSeqNo{0, 0, 0, true}
	}
	return seq.RedisString()
}

func (e *ExternalSeqNo) RedisString() string {
	return fmt.Sprintf("%d-%d", e.Time, e.Seq)
}

// IsAfter returns true if this ExternalSeqNo is after the other.
// ordering is by time then sequence,  then subsequence.  isLast is not considered.
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

// Helper method to turn a bool into a 1 or a zero
func boolToInt(b bool) int8 {
	if b {
		return 1
	} else {
		return 0
	}
}
