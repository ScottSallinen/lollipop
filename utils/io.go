package utils

import (
	"bytes"
	"math"
	"os"
	"unsafe"

	"github.com/rs/zerolog/log"
)

func init() {
	checkCompiler()
}

// Enforces a 64bit machine due to assumptions about size of ints.
func checkCompiler() {
	myInt := int(math.MaxInt64) // Shouldn't compile on a 32 bit system.
	myInt64 := int64(math.MaxInt64)
	if uint64(myInt) != uint64(myInt64) {
		panic("Must be on 64 bit system.")
	}
}

func OpenFile(path string) (file *os.File) {
	file, err := os.Open(path)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to open file: " + path)
	}
	return file
}

func CreateFile(path string) (file *os.File) {
	file, err := os.Create(path)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to create file: " + path)
	}
	return file
}

func ToInt(buf []byte) (n uint32) {
	for i := 0; i < len(buf); i++ {
		n = n*10 + uint32(buf[i]-'0')
	}
	return
}

func ToIntStr(buf string) (n uint32) {
	for i := 0; i < len(buf); i++ {
		n = n*10 + uint32(buf[i]-'0')
	}
	return
}

// var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}
const SPACE_MASK = 1<<9 | 1<<10 | 1<<11 | 1<<12 | 1<<13 | 1<<32

func isByteSpace(b byte) bool {
	return ((SPACE_MASK & (1 << b)) != 0)
}

// This is mostly copied from the standard library. Better versions below...
// ASCII only, no re-allocation. Assumes fieldBuff is large enough. Points to entries in byteBuff.
func FastFields(fieldBuff []string, byteBuff []byte) {
	fieldIndex := 0
	i := 0
	// Skip spaces in the front of the input.
	for i < len(byteBuff) && isByteSpace(byteBuff[i]) {
		i++
	}
	fieldStart := i
	for i < len(byteBuff) {
		if !isByteSpace(byteBuff[i]) {
			i++
			continue
		}
		b := byteBuff[fieldStart:i]
		fieldBuff[fieldIndex] = *(*string)(Noescape(unsafe.Pointer(&b)))
		fieldIndex++

		i++
		// Skip spaces in between fields.
		for i < len(byteBuff) && isByteSpace(byteBuff[i]) {
			i++
		}
		fieldStart = i
	}
	if fieldStart < len(byteBuff) { // Last field might end at EOF.
		b := byteBuff[fieldStart:]
		fieldBuff[fieldIndex] = *(*string)(Noescape(unsafe.Pointer(&b)))
	}
}

/*
// TODO: why is this not faster?! Escape analysis?!
// ASCII only, no re-allocation. Assumes fieldBuff is large enough. Points to entries in byteBuff.
func FastFieldsInline(fieldBuff []string, byteBuff []byte) (skipSpace bool, fieldStart int, fieldIndex int) {
	for i := 0; i < len(byteBuff); i++ {
		if skipSpace != ((SPACE_MASK & (1 << byteBuff[i])) != 0) {
			continue
		} else if skipSpace {
			byteRange := byteBuff[fieldStart:i]
			fieldBuff[fieldIndex] = *(*string)((unsafe.Pointer(&byteRange)))
			fieldIndex++
		} else {
			fieldStart = i
		}
		skipSpace = !skipSpace
	}
	if skipSpace && fieldStart < len(byteBuff) { // Last field might end at EOF.
		byteRange := byteBuff[fieldStart:]
		fieldBuff[fieldIndex] = *(*string)((unsafe.Pointer(&byteRange)))
	}
	return
}
*/

type FastFileLines struct {
	Buf   []byte
	Start int // First non-processed byte in buf.
	End   int // End of data in buf.
}

// Advance to the next token
func (s *FastFileLines) Scan(file *os.File) []byte {
	var err error
	for { // Until we have a token.
		// Must read more data. Shift data to beginning of buffer if there's lots of empty space.
		if s.Start > 0 && s.Start > len(s.Buf)/2 {
			for i := 0; i < s.End-s.Start; i++ {
				s.Buf[i] = s.Buf[s.Start+i]
			}
			s.End -= s.Start
			s.Start = 0
		}

		// Buffer is full: give up.
		if s.End == len(s.Buf) {
			panic("token too long")
		}
		// We can read some input.
		var n int
		for loop := 0; ; loop++ {
			n, err = file.Read(s.Buf[s.End:len(s.Buf)])
			s.End += n
			if n > 0 || err != nil {
				break
			}
			if loop > 100 {
				panic("no progress")
			}
		}
		if s.End > s.Start { // See if we can get a token with what we already have.
			if i := bytes.IndexByte(s.Buf[s.Start:s.End], '\n'); i >= 0 {
				token := s.Buf[s.Start : s.Start+i]
				s.Start += i + 1
				return token
			}
		}
		// We cannot generate a token with what we are holding.
		if err != nil {
			// We have reached EOF. Return whatever is left.
			if s.End > s.Start {
				i := s.Start
				s.Start = s.End
				return s.Buf[i:s.End]
			}
			return nil
		}
	}
}
