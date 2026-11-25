//go:build unix

package mpibootstrap

import (
	"bufio"
	"encoding/json"
	"io"
	"sync"
)

// message is the single type exchanged, one JSON document per line, on both kinds of connection:
// between a worker and the launcher, and between the agent and the launcher.
type message struct {
	// Hostname is sent by a worker when it calls home, and by the agent to name the worker to run Command on.
	Hostname string `json:"hostname,omitempty"`
	// Command is a shell command line. The agent sends it to the launcher, which sends it on to the worker.
	Command string `json:"command,omitempty"`
	// Output is a chunk of the output of the command, sent back by the worker while the command runs.
	Output []byte `json:"output,omitempty"`
	// Exited is sent once the command has ended, with ExitCode holding its exit code.
	Exited   bool `json:"exited,omitempty"`
	ExitCode int  `json:"exitCode,omitempty"`
	// Dismissed is sent by the launcher to a worker standing by, in place of a Command, once the job has succeeded:
	// the worker has nothing left to do and ends successfully.
	Dismissed bool `json:"dismissed,omitempty"`
	// Error reports that the request could not be served, e.g., a worker that is not part of the job.
	Error string `json:"error,omitempty"`
}

// channel sends and receives messages over a connection. Sends may be made from several goroutines.
type channel struct {
	conn    io.ReadWriteCloser
	reader  *bufio.Reader
	sendMux sync.Mutex
}

func newChannel(conn io.ReadWriteCloser) *channel {
	return &channel{conn: conn, reader: bufio.NewReader(conn)}
}

func (c *channel) send(m message) error {
	encoded, err := json.Marshal(m)
	if err != nil {
		return err
	}
	c.sendMux.Lock()
	defer c.sendMux.Unlock()
	_, err = c.conn.Write(append(encoded, '\n'))
	return err
}

func (c *channel) receive() (message, error) {
	var m message
	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		return m, err
	}
	err = json.Unmarshal(line, &m)
	return m, err
}

func (c *channel) close() {
	_ = c.conn.Close()
}

// outputWriter sends everything written to it as Output messages.
type outputWriter struct {
	channel *channel
}

func (w outputWriter) Write(p []byte) (int, error) {
	if err := w.channel.send(message{Output: p}); err != nil {
		return 0, err
	}
	return len(p), nil
}
