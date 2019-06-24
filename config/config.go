package config

import fmt "fmt"

// Less returns whether the receiver epoch is less than the other epoch.
func (m TestEpoch) Less(o TestEpoch) bool {
	if m.ProcessNanos == o.ProcessNanos {
		return m.BenchIter < o.BenchIter
	}
	return m.ProcessNanos < o.ProcessNanos
}

func (m TestEpoch) String() string {
	return fmt.Sprintf("%d/%d", m.ProcessNanos, m.BenchIter)
}
