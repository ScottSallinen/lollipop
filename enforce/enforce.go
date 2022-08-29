package enforce

import (
	"fmt"
	"log"
	"math"
)

func init() {
	checkCompiler()
}

// ENFORCE helper to halt program on error
func ENFORCE(query interface{}, args ...interface{}) {
	switch t := query.(type) {
	case bool:
		{
			if !t {
				log.Println("ENFORCE:", args)
				panic(0)
			}
		}
	case error:
		{
			if t != nil {
				log.Println("ENFORCE:", args)
				panic(t)
			}
		}
	case string:
		{
			log.Println("ENFORCE:", query.(string), args)
			panic(t)
		}
	case nil:
		// Allow nil to pass since we sometimes do enforce.ENFORCE(err) to ensure there is no error
		break
	default:
		log.Println("ENFORCE: incorrect usage of enforce with type: ", fmt.Sprintf("%T", t), "-", t, "-", args)
		panic(t)
	}
}

// checkCompiler Enforces a 64bit machine due to assumptions about sizeof(int).
func checkCompiler() {
	myint := int(math.MaxInt64) // Shouldn't compile on a 32 bit system.
	myint64 := int64(math.MaxInt64)
	ENFORCE(uint64(myint) == uint64(myint64), "Must be on 64 bit system.")
}
