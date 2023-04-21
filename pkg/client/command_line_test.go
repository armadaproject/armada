package client

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// writeTempConfig writes data to a temporary file, and returns the path to the created file,
// a cleanup function, and, if something went wrong, an error.
func writeTemp(data []byte) (string, func(), error) {
	dir := os.TempDir()
	dir, err := os.MkdirTemp(dir, "")
	if err != nil {
		return "", nil, fmt.Errorf("[writeTemp] error creating temp. directory: %s", err)
	}
	cleanup := func() {
		os.RemoveAll(dir)
	}

	file, err := os.CreateTemp(dir, "")
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("[writeTemp] error creating temp. file: %s", err)
	}
	path := file.Name()

	_, err = file.Write(data)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("[writeTemp] error writing config to %s: %s", path, err)
	}
	if err := file.Sync(); err != nil {
		return path, cleanup, err
	}

	return path, cleanup, nil
}

func TestLoadClientConfig(t *testing.T) {
	tests := map[string]struct {
		valid bool                // true if loading the config given in data should return an error
		data  []byte              // config data
		want  *armadaClientConfig // ground truth
	}{
		"valid": {
			valid: true,
			data: []byte(`
apiVersion: 1
currentContext: context1
clusters:
  - cluster1:
      server: 127.0.0.1:50051
  - cluster2:
      server: armada:50051
users:
  - user1:
      username: Bob
      password: 123456
  - user2:
      username: Alice
      password: foo	  
  - user2:
      username: DuplicateAlice
      password: i<3armada	  
contexts:
  - context1:
      cluster: cluster1
      user: user2`),
			want: &armadaClientConfig{
				ApiVersion:     "1",
				CurrentContext: "context1",
				Clusters: map[string]*clusterConfig{
					"cluster1": {Server: "127.0.0.1:50051"},
					"cluster2": {Server: "armada:50051"},
				},
				Users: map[string]*userConfig{
					"user1": {Username: "Bob", Password: "123456"},
					"user2": {Username: "DuplicateAlice", Password: "i<3armada"},
				},
				Contexts: map[string]*contextConfig{
					"context1": {Cluster: "cluster1", User: "user2"},
				},
			},
		},
		"missing currentContext": {
			valid: false,
			data: []byte(`
apiVersion: 1
currentContext: context1`),
			want: nil,
		},
		"missing user": {
			valid: false,
			data: []byte(`
apiVersion: 1
currentContext: context1
contexts:
  - context1:
      cluster: cluster1
      user: user2`),
			want: nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			path, cleanup, err := writeTemp(tc.data)
			if err != nil {
				t.Fatalf("error writing test config: %s", err)
			}
			defer cleanup()

			config, err := LoadClientConfig(path)
			if tc.valid {
				if err != nil {
					t.Fatalf("error loading client config: %s", err)
				}
				diff := cmp.Diff(tc.want, config)
				if diff != "" {
					t.Fatalf(diff)
				}
			} else {
				if err == nil {
					t.Fatalf("expected an error, but got none")
				}
			}
		})
	}
}
