package health

// TODO No point in having this in a separate file. Just consolidate the health package into 1 or 2 files.
type Checker interface {
	Check() error
}
