package accumulator

import (
	"fmt"
	"testing"
)

func TestDetectRow(t *testing.T) {
	for i := uint8(0); i < 32; i++ {
		for j := uint64(0); j < (1 << i); j++ {
			Orig := detectRowOrig(j, i)
			New := detectRow(j, i)
			if Orig != New {
				fmt.Printf("For row #%d\nOrig: %d\nNew: %d\n", i, Orig, New)
				t.Fatal("detectRow and detectRowOrig are not the same")
			}
		}

	}
	// Test higher tree rows
	for i := uint8(33); i <= 63; i++ {
		// only do 100,000 leaves
		for j := uint64(0); j < 100000; j++ {
			Orig := detectRowOrig(j, i)
			New := detectRow(j, i)
			if Orig != New {
				fmt.Printf("For row #%d\nOrig: %d\nNew: %d\n", i, Orig, New)
				t.Fatal("detectRow and detectRowOrig are not the same")
			}

		}

	}
}
func detectRowOrig(position uint64, forestRows uint8) uint8 {
	marker := uint64(1 << forestRows)
	var h uint8
	for h = 0; position&marker != 0; h++ {
		marker >>= 1
	}
	return h
}

func BenchmarkDetectRow_One(b *testing.B)     { benchmarkDetectRow(1, b) }
func BenchmarkDetectRow_Mil(b *testing.B)     { benchmarkDetectRow(1000000, b) }
func BenchmarkDetectRow_FiveMil(b *testing.B) { benchmarkDetectRow(5000000, b) }
func BenchmarkDetectRow_TenMil(b *testing.B)  { benchmarkDetectRow(10000000, b) }
func BenchmarkDetectRow_FifMil(b *testing.B)  { benchmarkDetectRow(50000000, b) }

func BenchmarkOrigDetectRow_One(b *testing.B)     { benchmarkDetectRowOrig(1, b) }
func BenchmarkOrigDetectRow_Mil(b *testing.B)     { benchmarkDetectRowOrig(1000000, b) }
func BenchmarkOrigDetectRow_FiveMil(b *testing.B) { benchmarkDetectRowOrig(5000000, b) }
func BenchmarkOrigDetectRow_TenMil(b *testing.B)  { benchmarkDetectRowOrig(10000000, b) }
func BenchmarkOrigDetectRow_FifMil(b *testing.B)  { benchmarkDetectRowOrig(50000000, b) }

func benchmarkDetectRow(n uint64, b *testing.B) {
	for i := uint8(33); i <= 63; i++ {
		for j := uint64(0); j < n; j++ {
			detectRow(j, i)

		}

	}
}

func benchmarkDetectRowOrig(n uint64, b *testing.B) {
	for i := uint8(33); i <= 63; i++ {
		for j := uint64(0); j < n; j++ {
			detectRow(j, i)

		}

	}
}

func TestTreeRows(t *testing.T) {
	// Test all the powers of 2
	for i := uint8(1); i <= 63; i++ {
		nLeaves := uint64(1 << i)
		Orig := treeRowsOrig(nLeaves)
		New := treeRows(nLeaves)
		if Orig != New {
			fmt.Printf("for n: %d;orig is %d. new is %d\n", nLeaves, Orig, New)
			t.Fatal("treeRows and treeRowsOrig are not the same")
		}

	}
	// Test billion leaves
	for n := uint64(0); n <= 100000000; n++ {
		Orig := treeRowsOrig(n)
		New := treeRows(n)
		if Orig != New {
			fmt.Printf("for n: %d;orig is %d. new is %d\n", n, Orig, New)
			t.Fatal("treeRows and treeRowsOrig are not the same")
		}
	}
}

// This is the orginal code for getting treeRows. The new function is tested
// against it.
func treeRowsOrig(n uint64) (e uint8) {
	// Works by iteratations of shifting left until greater than n
	for ; (1 << e) < n; e++ {
	}
	return
}

func BenchmarkTreeRows_HunThou(b *testing.B) { benchmarkTreeRows(100000, b) }
func BenchmarkTreeRows_Mil(b *testing.B)     { benchmarkTreeRows(1000000, b) }
func BenchmarkTreeRows_Bil(b *testing.B)     { benchmarkTreeRows(10000000, b) }
func BenchmarkTreeRows_Tril(b *testing.B)    { benchmarkTreeRows(100000000, b) }

func BenchmarkOrigTreeRows_HunThou(b *testing.B) { benchmarkTreeRowsOrig(100000, b) }
func BenchmarkOrigTreeRows_Mil(b *testing.B)     { benchmarkTreeRowsOrig(1000000, b) }
func BenchmarkOrigTreeRows_Bil(b *testing.B)     { benchmarkTreeRowsOrig(10000000, b) }
func BenchmarkOrigTreeRows_Tril(b *testing.B)    { benchmarkTreeRowsOrig(100000000, b) }

func benchmarkTreeRows(i uint64, b *testing.B) {
	for n := uint64(1000000); n < i+1000000; n++ {
		treeRows(n)
	}
}

func benchmarkTreeRowsOrig(i uint64, b *testing.B) {
	for n := uint64(1000000); n < i+1000000; n++ {
		treeRowsOrig(n)
	}
}
