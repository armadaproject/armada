package interfaces

type MinimalJob interface {
	Annotations() map[string]string
	PriorityClassName() string
}
