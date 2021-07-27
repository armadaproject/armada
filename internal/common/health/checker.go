package health

type Checker interface {
	Check() error
}
