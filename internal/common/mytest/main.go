package main

import (
	"fmt"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/apache/pulsar-client-go/pulsar"
)

func main() {
	mmm := map[pulsar.MessageID]string{}
	msg := pulsarutils.NewMessageId(1)
	msg2 := pulsarutils.NewMessageId(1)
	mmm[msg] = "hello"
	mmm[msg2] = "world"
	fmt.Println(mmm)
}
