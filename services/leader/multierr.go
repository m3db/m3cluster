package leader

import "strings"

type multiErr struct {
	errs []error
}

func newMultiError(errs ...error) error {
	if len(errs) == 0 {
		return nil
	}
	return multiErr{errs: errs}
}

func (m multiErr) Error() string {
	errStrs := make([]string, len(m.errs))
	for i, err := range m.errs {
		if err != nil {
			errStrs[i] = err.Error()
		}
	}
	return strings.Join(errStrs, "; ")
}
