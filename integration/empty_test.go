package integration

import "testing"

// Needed so that `integration` package is always buildable, since its main
// content is behind the `+build integration` flag.
func TestEmpty(t *testing.T) {

}
