package encoding

import (
	"github.com/cespare/xxhash/v2"
)

func XxhashHashIndex(token []byte, totalLength int) uint64 {
	return xxhash.Sum64(token) % uint64(totalLength)
}
