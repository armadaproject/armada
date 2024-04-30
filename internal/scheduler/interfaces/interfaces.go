package interfaces

type MinimalJob interface {
	GetAnnotations() map[string]string
	GetPriorityClassName() string
}
