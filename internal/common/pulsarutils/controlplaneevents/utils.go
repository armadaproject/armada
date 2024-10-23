package controlplaneevents

func PreProcess[T any](x []T) ([]T, error) { return x, nil }

func RetrieveKey[T any](_ T) string { return "" }
