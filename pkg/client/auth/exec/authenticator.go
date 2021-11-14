package exec

import (
	"bytes"
	"context"
	"fmt"
	grpc_credentials "google.golang.org/grpc/credentials"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
)

type Authenticator struct {

	// Set by the config
	Cmd  string
	Args []string
	Env  []string

	// Stubbable for testing
	stdin       io.Reader
	stderr      io.Writer
	interactive bool
	environ     func() []string

	// Mutex  guards calling the plugin. Since the plugin could be
	// interactive we want to make sure it's only called once.
	mu sync.Mutex
}

func NewAuthenticator(cmd string, args []string, env []string, interactive bool) grpc_credentials.PerRPCCredentials {
	return Authenticator{
		Cmd:         cmd,
		Args:        args,
		Env:         env,
		stdin:       os.Stdin,
		stderr:      os.Stderr,
		interactive: interactive,
		environ:     os.Environ,
	}
}

func (a *Authenticator) getCreds() (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.getCredsLocked()
}

// refreshCredsLocked executes the plugin and reads the credentials from
// stdout. It must be called while holding the Authenticator's mutex.
func (a *Authenticator) getCredsLocked() (string, error) {
	env := append(a.environ(), a.Env...)
	stdout := &bytes.Buffer{}
	cmd := exec.Command(a.Cmd, a.Args...)
	cmd.Env = env
	cmd.Stderr = a.stderr
	cmd.Stdout = stdout
	if a.interactive {
		cmd.Stdin = a.stdin
	}

	err := cmd.Run()

	if err != nil {
		return "", fmt.Errorf("error retrieving credentials %w", err)
	}

	tok := strings.TrimSpace(string(stdout.Bytes()))

	if tok == "" {
		return "", fmt.Errorf("command didn't return a tocken")
	}

	return tok, nil
}

func (a Authenticator) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	tok, err := a.getCreds()
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"authorization": "Bearer " + tok,
	}, nil
}

func (a Authenticator) RequireTransportSecurity() bool {
	return false
}
