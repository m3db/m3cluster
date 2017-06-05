package leader

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiErr(t *testing.T) {
	err := newMultiError()
	assert.NoError(t, err)

	err = newMultiError([]error{}...)
	assert.NoError(t, err)

	err1 := errors.New("1")
	err2 := errors.New("2")
	err = newMultiError(err1, err2)
	assert.EqualError(t, err, "1; 2")
}
