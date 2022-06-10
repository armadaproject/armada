package main

import (
	"fmt"

	"github.com/pkg/errors"
)

func main() {
	var err error
	foo := errors.WithMessage(err, "foo")
	fmt.Println(foo)
	fmt.Println(foo.Error())
}
