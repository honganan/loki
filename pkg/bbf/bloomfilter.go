package bbf

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/grafana/loki/v3/pkg/bytesutil"
	"github.com/grafana/loki/v3/pkg/util/encoding"
)

// bloomFilterHashesCount is the number of different hashes to use for bloom filter.
const bloomFilterHashesCount = 6

// bloomFilterBitsPerItem is the number of bits to use per each token.
const bloomFilterBitsPerItem = 16

// bloomFilterMarshal appends marshaled bloom filter for tokens to dst and returns the result.
func bloomFilterMarshal(dst []byte, tokens []string) []byte {
	bf := getBloomFilter()
	bf.mustInit(tokens)
	dst = bf.marshal(dst)
	putBloomFilter(bf)
	return dst
}

type bloomFilter struct {
	idx           int
	shardCapacity int
	appended      uint64
	bits          []uint64
}

func (bf *bloomFilter) reset() {
	bf.idx = 0
	bf.shardCapacity = 0
	bf.appended = 0
	bits := bf.bits
	for i := range bits {
		bits[i] = 0
	}
	bf.bits = bits[:0]
}

// marshal appends marshaled bf to dst and returns the result.
func (bf *bloomFilter) marshal(dst []byte) []byte {
	bits := bf.bits
	for _, word := range bits {
		dst = encoding.MarshalUint64(dst, word)
	}
	dst = encoding.MarshalUint64(dst, bf.appended)
	dst = encoding.MarshalUint64(dst, uint64(bf.idx))
	return dst
}

// unmarshal unmarshals bf from src.
func (bf *bloomFilter) unmarshal(src []byte) error {
	if len(src)%8 != 0 {
		return fmt.Errorf("cannot unmarshal bloomFilter from src with size not multiple by 8; len(src)=%d", len(src))
	}
	bf.reset()
	wordsCount := len(src)/8 - 2
	bits := bf.bits
	if n := wordsCount - cap(bits); n > 0 {
		// Ensure the byte slice is of the required length, this should not happen
		// 查询时没有使用对象池，所以这里的情况会发生
		//level.Warn(util_log.Logger).Log("msg", "byte slice capacity is not enough", "cap", cap(bits), "required", wordsCount)
		bits = append(bits[:cap(bits)], make([]uint64, n)...)
	}
	if wordsCount < 0 {
		return fmt.Errorf("invalid wordsCount=%d; len(bits)=%d; len(src)=%d", wordsCount, len(bits), len(src))
	}
	bits = bits[:wordsCount]
	for i := range bits {
		bits[i] = encoding.UnmarshalUint64(src)
		src = src[8:]
	}
	bf.bits = bits

	bf.appended = encoding.UnmarshalUint64(src)
	src = src[8:]
	bf.idx = int(encoding.UnmarshalUint64(src))
	return nil
}

// mustInit initializes bf with the given tokens
func (bf *bloomFilter) mustInit(tokens []string) {
	bitsCount := len(tokens) * bloomFilterBitsPerItem
	wordsCount := (bitsCount + 63) / 64
	bits := bf.bits
	if n := wordsCount - cap(bits); n > 0 {
		bits = append(bits[:cap(bits)], make([]uint64, n)...)
	}
	bits = bits[:wordsCount]
	bloomFilterAdd(bits, tokens)
	bf.bits = bits
}

// Init with capacity
func NewBloomFilter(capacity, index int) *bloomFilter {
	bf := getBloomFilter()
	bf.idx = index
	bf.shardCapacity = capacity

	bitsCount := capacity * bloomFilterBitsPerItem
	wordsCount := (bitsCount + 63) / 64
	bits := bf.bits
	if n := wordsCount - cap(bits); n > 0 {
		bits = append(bits[:cap(bits)], make([]uint64, n)...)
	}
	bits = bits[:wordsCount]
	bf.bits = bits
	return bf
}

// Add adds the given tokens to the bloom filter bits
func (bf *bloomFilter) Add(tokens []string) {
	bloomFilterAdd(bf.bits, tokens)
	bf.appended += uint64(len(tokens))
}

// bloomFilterAdd adds the given tokens to the bloom filter bits
func bloomFilterAdd(bits []uint64, tokens []string) {
	maxBits := uint64(len(bits)) * 64
	var buf [8]byte
	hp := (*uint64)(unsafe.Pointer(&buf[0]))
	for _, token := range tokens {
		*hp = xxhash.Sum64(bytesutil.ToUnsafeBytes(token))
		for i := 0; i < bloomFilterHashesCount; i++ {
			hi := xxhash.Sum64(buf[:])
			(*hp)++
			idx := hi % maxBits
			i := idx / 64
			j := idx % 64
			mask := uint64(1) << j
			w := bits[i]
			if (w & mask) == 0 {
				bits[i] = w | mask
			}
		}
	}
}

// containsAll returns true if bf contains all the given tokens.
func (bf *bloomFilter) containsAll(tokens []string) bool {
	bits := bf.bits
	if len(bits) == 0 {
		return true
	}
	maxBits := uint64(len(bits)) * 64
	var buf [8]byte
	hp := (*uint64)(unsafe.Pointer(&buf[0]))
	for _, token := range tokens {
		*hp = xxhash.Sum64(bytesutil.ToUnsafeBytes(token))
		for i := 0; i < bloomFilterHashesCount; i++ {
			hi := xxhash.Sum64(buf[:])
			(*hp)++
			idx := hi % maxBits
			i := idx / 64
			j := idx % 64
			mask := uint64(1) << j
			w := bits[i]
			if (w & mask) == 0 {
				// The token is missing
				return false
			}
		}
	}
	return true
}

// containsAny returns true if bf contains at least a single token from the given tokens.
func (bf *bloomFilter) containsAny(tokens []string) bool {
	bits := bf.bits
	if len(bits) == 0 {
		return true
	}
	maxBits := uint64(len(bits)) * 64
	var buf [8]byte
	hp := (*uint64)(unsafe.Pointer(&buf[0]))
nextToken:
	for _, token := range tokens {
		*hp = xxhash.Sum64(bytesutil.ToUnsafeBytes(token))
		for i := 0; i < bloomFilterHashesCount; i++ {
			hi := xxhash.Sum64(buf[:])
			(*hp)++
			idx := hi % maxBits
			i := idx / 64
			j := idx % 64
			mask := uint64(1) << j
			w := bits[i]
			if (w & mask) == 0 {
				// The token is missing. Check the next token
				continue nextToken
			}
		}
		// It is likely the token exists in the bloom filter
		return true
	}
	return false
}

func getBloomFilter() *bloomFilter {
	v := bloomFilterPool.Get()
	return v.(*bloomFilter)
}

func putBloomFilter(bf *bloomFilter) {
	bf.reset()
	bloomFilterPool.Put(bf)
}

var bloomFilterPool = sync.Pool{
	New: func() interface{} {
		return &bloomFilter{
			bits: make([]uint64, 0, 251000),
		}
	},
}

func (bf *bloomFilter) String() string {
	return fmt.Sprintf("idx=%d, appended=%d, shardCapacity=%d", bf.idx, bf.appended, bf.shardCapacity)
}
