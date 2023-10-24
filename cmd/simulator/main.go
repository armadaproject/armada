package main

import (
	"fmt"
	"os"

)

func main() {
	if err := cmd.RootCmd().Execute(); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}
