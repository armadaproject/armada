package main

import (
	"bytes"
	"os/exec"
)

func main() {
	puginCmd := "ls"

	stdout := &bytes.Buffer{}
	cmd := exec.Command(puginCmd)
	//cmd.Env = env
	//cmd.Stderr = a.stderr
	cmd.Stdout = stdout

	err := cmd.Run()

	if err == nil {
		token := string(stdout.Bytes())
		print(token)
	} else {
		print(err)
	}
}
