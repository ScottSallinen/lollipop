package graph

import (
	"strconv"

	"github.com/ScottSallinen/lollipop/utils"
)

// /*
type RawType uint32

// Maps a raw type within the given length. Modulo is fine for integer raw types.
func (r RawType) Within(len uint32) uint32 {
	return uint32(r) % len
}

func (r RawType) String() string {
	return strconv.Itoa(int(r))
}

func (r RawType) Integer() uint32 {
	return uint32(r)
}

// Mostly for testing, this converts a given integer into a RawType (for compatibility with string raw types).
func AsRawType(val int) RawType {
	return RawType(val)
}

// For testing / parsing, this converts a string to the RawType.
func AsRawTypeString(val string) RawType {
	return RawType(utils.ToIntStr(val))
}

// */

// Use this for string raw types.

/*
type RawType string

// Maps a raw type within the given length. For string raw types, we consistent-hash.
func (r RawType) Within(len uint32) uint32 {
	h := fnv.New32()
	h.Write([]byte(r))
	return h.Sum32() % len
}

func (r RawType) String() string {
	return string(r)
}

// Shouldn't be used.. just for compatibility with tests. If its an integer, use integer format to begin with -- integer input does not need to be zero indexed.
func (r RawType) Integer() uint32 {
	i, err := strconv.Atoi(string(r))
	if err != nil {
		panic(err)
	}
	return uint32(i)
}

// Mostly for testing, this converts a given integer into a string representation.
func AsRawType(val int) RawType {
	return RawType(strconv.Itoa(val))
}

// For testing / parsing, this converts a string to the RawType. Note we clone the string. (Input parsing which uses this is ephemeral.)
func AsRawTypeString(val string) RawType {
	return RawType(strings.Clone(val))
}

*/

// TODO: how to choose between options in a clean way?

// [src dst] , implying add only
func EdgeParser[E EPI[E]](stringFields []string) (TopologyEvent[E], []string) {
	return TopologyEvent[E]{
		TypeAndEventIdx: uint64(ADD),
		SrcRaw:          AsRawTypeString(stringFields[0]),
		DstRaw:          AsRawTypeString(stringFields[1]),
	}, stringFields[2:]
}

// [change src dst] , e.g., [a 1 2], [d 1 2]
/*
func EdgeParser[E EPI[E]](stringFields []string) (event TopologyEvent[E], remaining []string) {
	event.Type = ADD
	switch stringFields[0][0] {
	case 'a':
		event.Type = ADD
	case 'd':
		event.Type = DEL
	default:
		log.Panic().Msg("Unknown change type: " + utils.V(stringFields[0]))
	}

	event.SrcRaw = utils.ToIntStr(stringFields[1])
	event.DstRaw = utils.ToIntStr(stringFields[2])
	return event, stringFields[3:]
}
*/
