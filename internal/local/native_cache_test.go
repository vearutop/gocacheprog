package local

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHumanBytesPerSecondBinary(t *testing.T) {
	require.Equal(t, "0 B/s", humanBytesPerSecondBinary(0, time.Second))
	require.Equal(t, "0 B/s", humanBytesPerSecondBinary(1024, 0))
	require.Equal(t, "1.0 KiB/s", humanBytesPerSecondBinary(2048, 2*time.Second))
	require.Equal(t, "1.5 KiB/s", humanBytesPerSecondBinary(1536, time.Second))
}
