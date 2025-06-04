package bbf

import (
	"sync"
	"unsafe"
)

type SimpleSplitTokenizer struct{}

func (sst *SimpleSplitTokenizer) Tokens(line string) []string {
	m := sst.Tokenize(line)
	tokens := make([]string, 0, len(m))
	for token := range m {
		tokens = append(tokens, token)
	}
	return tokens
}

func (sst *SimpleSplitTokenizer) Tokenize(str string) map[string]struct{} {
	t := getTokenizer()

	m := t.m
	tokenizeString(m, str)
	tokens := make(map[string]struct{}, len(m))
	for token := range m {
		tokens[token] = struct{}{}
	}

	putTokenizer(t)
	return tokens
}

type tokenizer struct {
	m map[string]struct{}
}

var tokenizerPool sync.Pool

func (t *tokenizer) reset() {
	m := t.m
	for k := range m {
		delete(m, k)
	}
}

func getTokenizer() *tokenizer {
	v := tokenizerPool.Get()
	if v == nil {
		return &tokenizer{
			m: make(map[string]struct{}, 2048),
		}
	}
	return v.(*tokenizer)
}

func putTokenizer(t *tokenizer) {
	t.reset()
	tokenizerPool.Put(t)
}

func tokenizeString(dst map[string]struct{}, s string) {
	for len(s) > 0 {
		// Search for the next token.
		nextIdx := len(s)
		for i, c := range s {
			if isTokenRune(c) {
				nextIdx = i
				break
			}
		}
		s = s[nextIdx:]
		// Search for the end of the token
		nextIdx = len(s)
		for i, c := range s {
			if !isTokenRune(c) {
				nextIdx = i
				break
			}
		}

		if len(s[:nextIdx]) > 0 {
			dst[s[:nextIdx]] = struct{}{}
		}
		s = s[nextIdx:]
	}
}

var tokenLookupTable = initTokenTable()

func initTokenTable() [256]bool {
	var table [256]bool
	for c := 'a'; c <= 'z'; c++ {
		table[c] = true
	}
	for c := 'A'; c <= 'Z'; c++ {
		table[c] = true
	}
	for c := '0'; c <= '9'; c++ {
		table[c] = true
	}
	return table
}

func isTokenRune(c rune) bool {
	return c < 256 && tokenLookupTable[c]
}

// StringToBytes converts string to byte slice. (copied from vendor/github.com/go-redis/redis/v8/internal/util/unsafe.go)
func StringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
