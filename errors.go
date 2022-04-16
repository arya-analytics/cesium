package caesium

type Error struct {
	Type    ErrorType
	Message string
	Base    error
}

func (e Error) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Base != nil {
		return e.Base.Error()
	}
	return "caesium - no Error message"
}

//go:generate stringer --type=ErrorType --output=errors_string.go
type ErrorType byte

const (
	ErrUnknown ErrorType = iota
	ErrInternal
	ErrInvalidQuery
	ErrChannelLock
)

func newDerivedError(t ErrorType, base error) error {
	return Error{Type: t, Message: base.Error(), Base: base}
}

func newSimpleError(t ErrorType, msg string) error {
	return Error{Type: t, Message: msg}
}

func newUnknownError(base error) error {
	return newDerivedError(ErrUnknown, base)
}
