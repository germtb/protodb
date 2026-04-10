package protodb

import (
	"fmt"
	"testing"
)

// Compares: N individual Gets vs one BulkGet with N keys.
// "Clustered" means keys are adjacent (e.g., 0..N-1).
// "Sparse" means keys are spread out (e.g., every 10th).
func BenchmarkBulkGetVsIndividualGet(b *testing.B) {
	setup := func(b *testing.B) *Engine {
		engine, _ := Open(b.TempDir())
		for idx := uint64(0); idx < 10000; idx++ {
			engine.Put(key(idx), []byte("value"))
		}
		engine.Flush()
		engine.Compact()
		return engine
	}

	// Granular sweep around the suspected crossover point
	for _, n := range []int{5, 10, 15, 20, 25, 30, 40, 50, 75, 100, 200, 500, 1000} {
		// Clustered: keys 0..n-1 (adjacent)
		b.Run(fmt.Sprintf("Clustered/Individual_%d", n), func(b *testing.B) {
			engine := setup(b)
			defer engine.Close()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				for idx := 0; idx < n; idx++ {
					engine.Get(key(uint64(idx)))
				}
			}
		})

		b.Run(fmt.Sprintf("Clustered/Bulk_%d", n), func(b *testing.B) {
			engine := setup(b)
			defer engine.Close()
			keys := make([]Key, n)
			for idx := 0; idx < n; idx++ {
				keys[idx] = key(uint64(idx))
			}
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				engine.BulkGet(keys)
			}
		})

		// Sparse: every 10th key
		b.Run(fmt.Sprintf("Sparse/Individual_%d", n), func(b *testing.B) {
			engine := setup(b)
			defer engine.Close()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				for idx := 0; idx < n; idx++ {
					engine.Get(key(uint64(idx * 10)))
				}
			}
		})

		b.Run(fmt.Sprintf("Sparse/Bulk_%d", n), func(b *testing.B) {
			engine := setup(b)
			defer engine.Close()
			keys := make([]Key, n)
			for idx := 0; idx < n; idx++ {
				keys[idx] = key(uint64(idx * 10))
			}
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				engine.BulkGet(keys)
			}
		})
	}
}
