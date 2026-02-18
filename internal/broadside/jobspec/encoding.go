package jobspec

import "time"

const crockfordBase32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

var base32Lookup = func() [256]int8 {
	var table [256]int8
	for i := range table {
		table[i] = -1
	}
	for i, c := range crockfordBase32 {
		table[c] = int8(i)
	}
	return table
}()

func EncodeJobID(t time.Time, queueIdx, jobSetIdx, sequence int) string {
	var buf [18]byte

	encodeBase32Into(buf[0:10], uint64(t.UnixMilli()))

	encodeBase32Into(buf[10:12], uint64(queueIdx))

	encodeBase32Into(buf[12:14], uint64(jobSetIdx))

	encodeBase32Into(buf[14:18], uint64(sequence))

	return string(buf[:])
}

func EncodeRunID(jobID string, runSeq int) string {
	var buf [20]byte
	copy(buf[0:18], jobID)
	encodeBase32Into(buf[18:20], uint64(runSeq))
	return string(buf[:])
}

func encodeBase32Into(dst []byte, val uint64) {
	for i := len(dst) - 1; i >= 0; i-- {
		dst[i] = crockfordBase32[val&0x1f]
		val >>= 5
	}
}

func ExtractJobNumber(jobID string) int {
	if len(jobID) < 18 {
		return 0
	}
	return int(decodeBase32(jobID[14:18]))
}

func decodeBase32(s string) uint64 {
	var result uint64
	for i := 0; i < len(s); i++ {
		result <<= 5
		val := base32Lookup[s[i]]
		if val >= 0 {
			result |= uint64(val)
		}
	}
	return result
}
