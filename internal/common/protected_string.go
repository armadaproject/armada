package common

// Harder to print out string
type ProtectedString string

func (b ProtectedString) String() string {
	return "***"
}
