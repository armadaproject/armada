package validation

type Validator[T any] interface {
	Validate(obj T) error
}

type CompoundValidator[T any] struct {
	validators []Validator[T]
}

func NewCompoundValidator[T any](validators ...Validator[T]) CompoundValidator[T] {
	return CompoundValidator[T]{
		validators: validators,
	}
}

func (c CompoundValidator[T]) Validate(obj T) error {
	for _, v := range c.validators {
		err := v.Validate(obj)
		if err != nil {
			return err
		}
	}
	return nil
}
