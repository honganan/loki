package bbf

import (
	"testing"
)

func TestTokenizeStrings(t *testing.T) {
	f := func(a string, tokensExpected []string) {
		t.Helper()
		tn := getTokenizer()

		m := tn.m
		tokenizeString(m, a)
		for _, token := range tokensExpected {
			if _, ok := m[token]; !ok {
				t.Fatalf("unfounded tokens: %s", token)
			}
		}
	}
	f("", nil)
	f("foo", []string{"foo"})
	f("2536f94a134111ef858dea4b886c3cc5", []string{"2536f94a134111ef858dea4b886c3cc5"})
	f("foo bar---.!!([baz]!!! %$# TaSte", []string{"foo", "bar", "baz", "TaSte"})
	f("1234 f12. 34 f12 AS", []string{"1234", "f12", "34", "AS"})
	f(`
Apr 28 13:43:38 localhost whoopsie[2812]: [13:43:38] online
Apr 28 13:45:01 localhost CRON[12181]: (root) CMD (command -v debian-sa1 > /dev/null && debian-sa1 1 1)
Apr 28 13:48:01 localhost kernel: [36020.497806] CPU0: Core temperature above threshold, cpu clock throttled (total events = 22034)
`, []string{"Apr", "28", "13", "43", "38", "localhost", "whoopsie", "2812", "online", "45", "01", "CRON", "12181",
		"root", "CMD", "command", "v", "debian", "sa1", "dev", "null", "1", "48", "kernel", "36020", "497806", "CPU0", "Core",
		"temperature", "above", "threshold", "cpu", "clock", "throttled", "total", "events", "22034"})
}

func BenchmarkIsTokenRune(b *testing.B) {
	initTokenTable()
	runes := []rune{'a', 'b', 'c', 'd', '1', '2', '_', ' ', '!', '中'}
	for i := 0; i < b.N; i++ {
		for _, r := range runes {
			isTokenRune(r)
		}
	}
}

func BenchmarkStringToBytes(b *testing.B) {
	p := initStrings(1000)
	bs := make([]byte, 1024)
	b.ResetTimer()
	for i := 0; i < len(p); i++ {
		bs = StringToBytes(p[i])
	}
	if len(bs) > 0 {
		_ = bs[0]
	}
}

func BenchmarkStringToBytes2(b *testing.B) {
	p := initStrings(1000000)
	bs := make([]byte, 1024)
	b.ResetTimer()
	for i := 0; i < len(p); i++ {
		bs = []byte(p[i])
	}
	if len(bs) > 0 {
		_ = bs[0]
	}
}

const STR = "hello worldhello world hello worldhello worldhello worldhello worldhello worldhello worldhello worldhello worldhello worldhello worldhello worldhello"

func initStrings(N int) []string {
	s := make([]string, N)
	for i := 0; i < N; i++ {
		s[i] = STR
	}
	return s
}
