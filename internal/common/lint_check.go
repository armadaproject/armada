package common

import "fmt"

func tEST() error {
	return fmt.Errorf("tEST error")
}

func tryTEST() {
	err := tEST()
	fmt.Println(err.Error())
	err = tEST()
}
