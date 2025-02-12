package utils

import (
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/exp/constraints"
)

type Pair[F any, S any] struct {
	First  F
	Second S
}

// Further tuning is needed for performance...
func BackOff(count int) {
	//if count == 0 {
	//runtime.Gosched()
	//} else {
	if count > 2000 {
		count = 2000
	}
	time.Sleep(time.Duration((count+1)*100) * time.Microsecond)
	// if count > 2000 {
	// 	log.Panic().Msg("BackOff count too high")
	// }
	//}
}

// An imprecise float approximate comparison. "optional" variance with ... args strategy
func FloatEquals(a float64, b float64, inputVariance ...float64) bool {
	variance := 0.001
	if len(inputVariance) >= 1 {
		variance = inputVariance[0]
	}
	return math.Abs(a-b) < variance
}

// Round up to the next power of 2
func RoundUpPow(i uint64) uint64 {
	i--
	i |= i >> 1
	i |= i >> 2
	i |= i >> 4
	i |= i >> 8
	i |= i >> 16
	i |= i >> 32
	i++
	return i
}

func Max[T constraints.Ordered](x, y T) T {
	if x < y {
		return y
	}
	return x
}

func Min[T constraints.Ordered](x, y T) T {
	if y < x {
		return y
	}
	return x
}

func MaxSlice[T constraints.Ordered](slice []T) T {
	max := slice[0]
	for i := range slice {
		max = Max(max, slice[i])
	}
	return max
}

func MinSlice[T constraints.Ordered](slice []T) T {
	min := slice[0]
	for i := range slice {
		min = Min(min, slice[i])
	}
	return min
}

func Sum[T constraints.Integer | constraints.Float](slice []T) (sum T) {
	for i := range slice {
		sum += slice[i]
	}
	return sum
}

func Median[T constraints.Integer | constraints.Float](n []T) T {
	return Percentile(n, 50)
}

func Percentile[T constraints.Integer | constraints.Float](n []T, percentile int, reverse ...bool) T {
	if len(n) == 0 {
		log.Warn().Msg("WARNING: Percentile called on empty slice")
		return 0
	}
	if len(n) == 1 {
		return n[0]
	}

	copyN := make([]T, len(n))
	copy(copyN, n)

	if reverse != nil && reverse[0] {
		sort.Slice(copyN, func(i, j int) bool { return copyN[i] > copyN[j] })
	} else {
		sort.Slice(copyN, func(i, j int) bool { return copyN[i] < copyN[j] })
	}
	idx := int(((float64(percentile) / 100.0) * float64(len(copyN))))
	if len(copyN)%2 == 0 || idx == 0 { // even number of elements, or the we are targeting the first element
		return copyN[idx]
	} else if copyN[idx-1] == copyN[idx] {
		return copyN[idx] // Just for the silly case where value is two max ints, don't add... TODO: overflow check.
	}
	return (copyN[idx-1] + copyN[idx]) / 2
}

func Shuffle[T any](slice []T) {
	for i := range slice {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func RemoveRandomElement[T any](slice []T) (T, []T) {
	idx := rand.Intn(len(slice))
	ret := slice[idx]
	slice[idx] = slice[len(slice)-1]
	return ret, slice[:len(slice)-1]
}

// Compares two arrays: showcases average and L1 differences.
// IgnoreSize: some number to ignore when computing 95th percentile. (i.e., for ignoring singletons)
// Returns: Average L1 diff, 50th percentile L1 diff, 95th percentile L1 diff
func ResultCompare[T constraints.Float | constraints.Integer](a []T, b []T, ignoreSize int) (avgL1Diff float64, medianL1Diff float64, percentile95L1 float64) {
	if len(a) == 0 {
		return
	}
	listL1Diff := make([]float64, len(a))

	for i := range a {
		//delta := math.Abs(float64((b[i])-a[i]) * 100.0 / float64(Min(a[idx], b[i])))
		//largestRelativeDiff = Max(largestRelativeDiff, delta)
		l1delta := math.Abs(float64(b[i] - a[i]))
		listL1Diff[i] = l1delta
		avgL1Diff += l1delta
	}
	avgL1Diff = avgL1Diff / float64(len(a))

	sort.Float64s(listL1Diff)

	medianIdx := (len(listL1Diff) - ignoreSize) / 2
	medianL1Diff = listL1Diff[medianIdx+ignoreSize]
	if len(listL1Diff)%2 == 1 { // odd
		medianL1Diff = (listL1Diff[medianIdx+ignoreSize-1] + listL1Diff[medianIdx+ignoreSize]) / 2
	}
	percentile95L1 = listL1Diff[int(float64(len(listL1Diff)-ignoreSize)*0.95)+ignoreSize]

	return avgL1Diff, medianL1Diff, percentile95L1
}
