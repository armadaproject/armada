package groups

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_decodeSID(t *testing.T) {
	hexSID := []byte("010500000000000515000000A065CF7E784B9B5FE77C8770091C0100")
	sid := "S-1-5-21-2127521184-1604012920-1887927527-72713"

	data := make([]byte, hex.DecodedLen(len(hexSID)))
	_, err := hex.Decode(data, hexSID)
	assert.NoError(t, err)
	assert.Equal(t, decodeSID(data), sid)
}
