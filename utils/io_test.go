package utils

import (
	"strconv"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

var testByteBuff = []byte("123 432 1 23421 100 2341\n")

func Benchmark_Fields_ToInt(b *testing.B) {
	var s1 []string
	ints := make([]uint32, 6)
	b.ResetTimer()
	accum := 0
	for i := 0; i < b.N; i++ {
		s1 = strings.Fields(string(testByteBuff))
		for j := 0; j < 6; j++ {
			si, _ := strconv.Atoi(s1[j])
			ints[j] = uint32(si)
		}
		// Do something with the ints
		a := Sum(ints)
		b := MaxSlice(ints)
		c := a + b
		accum += int(c)
	}
}

func Benchmark_FastFields_ToInt(b *testing.B) {
	s1 := make([]string, len(testByteBuff))
	ints := make([]uint32, len(testByteBuff))
	b.ResetTimer()
	accum := 0
	for i := 0; i < b.N; i++ {
		FastFields(s1, testByteBuff)
		for j := 0; j < len(testByteBuff); j++ {
			ints[j] = ToIntStr(s1[j])
		}
		// Do something with the ints
		a := Sum(ints)
		b := MaxSlice(ints)
		c := a + b
		accum += int(c)
	}
}

/*
func Benchmark_FastFieldsInline_ToInt(b *testing.B) {
	s1 := make([]string, len(testByteBuff))
	ints := make([]uint32, len(testByteBuff))
	b.ResetTimer()
	accum := 0
	for i := 0; i < b.N; i++ {
		FastFieldsInline(s1, testByteBuff)
		for j := 0; j < len(testByteBuff); j++ {
			ints[j] = ToIntStr(s1[j])
		}
		// Do something with the ints
		a := Sum(ints)
		b := MaxSlice(ints)
		c := a + b
		accum += int(c)
	}
}
*/

func expect[T comparable](t *testing.T, a T, b T) {
	if a != b {
		t.Error("Expected: ", a, " got: ", b)
	}
}

func Test_ToInt(t *testing.T) {
	b1 := make([]string, 6)
	ints := make([]uint32, 6)
	FastFields(b1, testByteBuff)
	for i := 0; i < 6; i++ {
		ints[i] = ToIntStr(b1[i])
	}
	expect(t, ints[0], uint32(123))
	expect(t, ints[1], uint32(432))
	expect(t, ints[2], uint32(1))
	expect(t, ints[3], uint32(23421))
	expect(t, ints[4], uint32(100))
	expect(t, ints[5], uint32(2341))
}

// Test various strings to ensure they get fielded properly
func Test_FastFields(t *testing.T) {
	a := make([]string, 10)
	// b := make([]string, 10)

	setOfByteBuffs := [][]byte{
		[]byte("hello world this is a test"),
		[]byte("hello world this is a test "),
		[]byte(" hello world this is a test"),
		[]byte("hello   world  this  is      a    test"),
		[]byte("  hello   world    this  is  a  test "),
		[]byte("hello\tworld\tthis\tis\ta\ttest"),
		[]byte("\thello world this is a test\t"),
		[]byte(" hello world this is a test\n\n"),
		[]byte("hello\t world\t this\t is\ta\ttest\r\n"),
	}

	for _, byteBuff := range setOfByteBuffs {
		FastFields(a, byteBuff)
		expect(t, a[0], "hello")
		expect(t, a[1], "world")
		expect(t, a[2], "this")
		expect(t, a[3], "is")
		expect(t, a[4], "a")
		expect(t, a[5], "test")

		/*
			FastFieldsInline(b, byteBuff)
			expect(t, b[0], "hello")
			expect(t, b[1], "world")
			expect(t, b[2], "this")
			expect(t, b[3], "is")
			expect(t, b[4], "a")
			expect(t, b[5], "test")
		*/
	}
}

func Benchmark_Space(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for r := rune(0); r <= utf8.MaxRune; r++ {
			isByteSpace(byte(r))
		}
	}
}

func Benchmark_UnicodeSpace(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for r := rune(0); r <= utf8.MaxRune; r++ {
			unicode.IsSpace(r)
		}
	}
}
