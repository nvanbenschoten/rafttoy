package util

import (
	"fmt"
	"testing"
)

// RunFor runs a series of subbenchmarks with the specified variable set to
// values starting at start, increasing by 2^exp each step, and consisting of n
// total steps.
func RunFor(b *testing.B, name string, start, exp, n int, f func(b *testing.B, i int)) {
	for i := 0; i < n; i++ {
		v := start * (1 << uint(exp*i))
		b.Run(fmt.Sprintf("%s=%d", name, v), func(b *testing.B) {
			f(b, v)
		})
	}
}
