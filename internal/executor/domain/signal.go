package domain

type PanicSignal struct {
}

func (signal PanicSignal) String() string {
	return "Panic signal"
}

func (signal PanicSignal) Signal() {}
