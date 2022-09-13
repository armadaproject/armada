package authorization

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/auth/configuration"
)

// A token
const (
	testToken = "eyJhbGciOiJSUzI1NiIsImtpZCI6IkRwWnhkblBHNm1oak1kNjNLU0VJc0cwMWctXzQ1YlU2aXFYeGNraG1xc2MifQ" +
		".eyJhdWQiOlsiaHR0cHM6Ly9rdWJlcm5ldGVzLmRlZmF1bHQuc3ZjLmNsdXN0ZXIubG9jYWwiXSwiZXhwIjoxNjYzMDY4NjYxL" +
		"CJpYXQiOjE2NjMwNjUwNjEsImlzcyI6Imh0dHBzOi8va3ViZXJuZXRlcy5kZWZhdWx0LnN2Yy5jbHVzdGVyLmxvY2FsIiwia3V" +
		"iZXJuZXRlcy5pbyI6eyJuYW1lc3BhY2UiOiJkZWZhdWx0Iiwic2VydmljZWFjY291bnQiOnsibmFtZSI6ImFkbWluLXVzZXIiL" +
		"CJ1aWQiOiIxMGVhZDI5OC1hMTU2LTRkM2QtOTUxMC1jMzNmNGI0NjU5MmQifX0sIm5iZiI6MTY2MzA2NTA2MSwic3ViIjoic3l" +
		"zdGVtOnNlcnZpY2VhY2NvdW50OmRlZmF1bHQ6YWRtaW4tdXNlciJ9.wcRLGXMXd2Ek6JQypyuvijw3pWObwQ1OIfwUMccFe4E0" +
		"8r6_DcmizemfYs2uYuwHovcgqsOqJR3OoMhZd3jo1Irl4XEoeEjqb1qioMl7zpXBcr7kohBUxYbJyWa_FehKsysbB80mw_SG8F" +
		"P3o-ZHo7wL8oE1X0WNoSnOz14ke7MR2ZK3v8E4YfbYVLxilk3E3bnVR3QtyVgo7vn-Y_AHKeZdjHD-sXE6Fb8jO48vvj54JwDW" +
		"-w7qDRYYLgalRU3DXfsil072h_PYMFwtE57NGRjRGyOR2HIZuCVADjt_bF8E87i1TG3ELMCtSE5Jr78lmJ2zhyBFrA_FhljJoKTZJg"
	testKid = "DpZxdnPG6mhjMd63KSEIsG01g-_45bU6iqXxckhmqsc"
	testUrl = "https://kubernetes.config.test:420"
)

func TestValidateKid(t *testing.T) {
	assert.Error(t, validateKid(""))
	assert.Error(t, validateKid("../a/dangerous/filepath"))
	assert.NoError(t, validateKid(testKid))
}

func TestParseTime(t *testing.T) {
	myTime, err := parseTime(testToken)
	assert.NoError(t, err)
	assert.Equal(t, time.Unix(1663068661, 0), myTime)
}

func TestGetClusterURL(t *testing.T) {
	// Setup environment
	tempdir, err := os.MkdirTemp("", "kid-mapping")
	defer os.Remove(tempdir)
	if err != nil {
		t.Errorf("TestGetClusterURL returned error: %s", err)
	}
	path := filepath.Join(tempdir, testKid)
	kidfile, err := os.Create(path)
	if err != nil {
		t.Errorf("TestGetClusterURL returned error: %s", err)
	}
	defer os.Remove(path)
	defer kidfile.Close()
	kidfile.Write([]byte(testUrl))

	testAuthService := NewKubernetesNativeAuthService(configuration.KubernetesAuthConfig{
		KidMappingFileLocation: tempdir,
	})

	url, err := testAuthService.getClusterURL(testToken)
	if err != nil {
		t.Errorf("TestGetClusterURL returned error: %s", err)
	}

	assert.Equal(t, testUrl, url)
}
