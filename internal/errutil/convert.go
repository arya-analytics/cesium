package errutil

// Convert is a function that receives an error and converts it into an error of another type.
// This function must meet the following requirements:
//
// 1. Returns true as second arg if the error was converted (i.e. "handled").
// 2. Returns nil (or original error) and false if error can't be converted.
//
type Convert func(error) (error, bool)

// ConvertChain chains and executes a set of Convert.
// Last Convert in chain considered "default" converter, as will be last executed.
// For example:
// 		err := errors.New("unconverted error")
// 		cc := errutil.ConvertChain{Convert1, Convert2, DefaultConvert}
// 		cErr := cc.QExec()
// 		fmt.Printf(cErr)
// Output:
// 		converted error.
type ConvertChain []Convert

// Exec executes ConvertChain on an error. If converted successfully, will return converted error. Otherwise,
// returns original error.
func (cc ConvertChain) Exec(err error) error {
	if err == nil {
		return nil
	}
	for _, c := range cc {
		if cErr, ok := c(err); ok {
			return cErr
		}
	}
	return err
}
