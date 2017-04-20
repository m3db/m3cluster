package client

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	cf := Configuration{}
	require.Equal(t, defaultInitTimeout, cf.NewOptions().InitTimeout())

	timeout := time.Second
	cf = Configuration{InitTimeout: &timeout}
	require.Equal(t, timeout, cf.NewOptions().InitTimeout())
}
