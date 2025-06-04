package bbf

import (
	"context"
	"sync"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/log/pattern"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
)

const matchMaxConcurrency = 20

type BloomTest interface {
	Matches(bloom *chkChecker) []*logproto.ChunkRef
}

type BloomTests []BloomTest

func (b BloomTests) Matches(cc *chkChecker) []*logproto.ChunkRef {
	var (
		results = make([][]*logproto.ChunkRef, 0, len(b))
		lock    sync.Mutex
	)
	err := concurrency.ForEachJob(context.Background(), len(b), matchMaxConcurrency, func(ctx context.Context, i int) error {
		rsp := b[i].Matches(cc)
		lock.Lock()
		results = append(results, rsp)
		lock.Unlock()
		return nil
	})
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "error running BloomTests.Matches", "err", err)
		return cc.refs
	}

	if len(results) == 1 {
		return results[0]
	}
	return intersectChunks(results)
}

// ExtractTestableLineFilters extracts all line filters from an expression
// that can be tested against a bloom filter. This will skip any line filters
// after a line format expression. A line format expression might add content
// that the query later matches against, which can't be tested with a bloom filter.
// E.g. For {app="fake"} |= "foo" | line_format "thisNewTextShouldMatch" |= "thisNewTextShouldMatch"
// this function will return only the line filter for "foo" since the line filter for "thisNewTextShouldMatch"
// wouldn't match against the bloom filter but should match against the query.
func ExtractTestableLineFilters(expr syntax.Expr) []syntax.LineFilterExpr {
	if expr == nil {
		return nil
	}

	var filters []syntax.LineFilterExpr
	var lineFmtFound bool
	visitor := &syntax.DepthFirstTraversal{
		VisitLineFilterFn: func(v syntax.RootVisitor, e *syntax.LineFilterExpr) {
			if e != nil && !lineFmtFound {
				filters = append(filters, *e)
			}
		},
		VisitLineFmtFn: func(v syntax.RootVisitor, e *syntax.LineFmtExpr) {
			if e != nil {
				lineFmtFound = true
			}
		},
	}
	expr.Accept(visitor)
	return filters
}

// FiltersToBloomTest converts a list of line filters to a BloomTest.
// Note that all the line filters should be testable against a bloom filter.
// Use ExtractTestableLineFilters to extract testable line filters from an expression.
// TODO(owen-d): limits the number of bloom lookups run.
// An arbitrarily high number can overconsume cpu and is a DoS vector.
// TODO(owen-d): use for loop not recursion to protect callstack
func FiltersToBloomTest(t Tokenizer, filters ...syntax.LineFilterExpr) BloomTest {
	tests := make(BloomTests, 0, len(filters))
	for _, f := range filters {
		if f.Left != nil {
			tests = append(tests, FiltersToBloomTest(t, *f.Left))
		}
		// TODO(anan): Or 是哪种表达式，和 regex 有什么区别，能不能支持？
		if f.Or != nil {
			left := FiltersToBloomTest(t, *f.Or)
			right := simpleFilterToBloomTest(t, f.LineFilter)
			tests = append(tests, newOrTest(left, right))
			continue
		}

		tests = append(tests, simpleFilterToBloomTest(t, f.LineFilter))
	}
	return tests
}

func simpleFilterToBloomTest(t Tokenizer, filter syntax.LineFilter) BloomTest {
	switch filter.Ty {
	case log.LineMatchNotEqual, log.LineMatchNotRegexp, log.LineMatchNotPattern:
		// We cannot test _negated_ filters with a bloom filter since blooms are probabilistic
		// filters that can only tell us if a string _might_ exist.
		// For example, for `!= "foo"`, the bloom filter might tell us that the string "foo" might exist
		// but because we are not sure, we cannot discard that chunk because it might actually not be there.
		// Therefore, we return a test that always returns true.
		return MatchAll
	case log.LineMatchEqual:
		return newStringTest(t, filter.Match)
	case log.LineMatchRegexp:
		return MatchAll
		// TODO anan: 我们的分词方式可能不能支持 pattern？
	case log.LineMatchPattern:
		return newPatternTest(t, filter.Match)
	default:
		return MatchAll
	}
}

type matchAllTest struct{}

var MatchAll = matchAllTest{}

func (n matchAllTest) Matches(cc *chkChecker) []*logproto.ChunkRef {
	return cc.refs
}

type Tokenizer interface {
	Tokens(line string) []string
}

type stringTest struct {
	tokens []string
}

func newStringTest(b Tokenizer, search string) (res BloomTest) {
	tokens := b.Tokens(search)
	return stringTest{tokens: tokens}
}

// Matches implements the BloomTest interface
func (b stringTest) Matches(cc *chkChecker) []*logproto.ChunkRef {
	var (
		results = make([][]*logproto.ChunkRef, 0, len(b.tokens))
		lock    sync.Mutex
	)

	err := concurrency.ForEachJob(context.Background(), len(b.tokens), matchMaxConcurrency, func(ctx context.Context, i int) error {
		// 太短的 token 直接跳过
		if len(b.tokens[i]) <= 1 {
			lock.Lock()
			results = append(results, cc.refs)
			lock.Unlock()
			return nil
		}
		rsp := cc.Test(b.tokens[i])
		lock.Lock()
		results = append(results, rsp)
		lock.Unlock()
		return nil
	})
	if err != nil {
		_ = level.Error(util_log.Logger).Log("msg", "error running stringTest.Matches", "err", err)
		return cc.refs
	}
	return intersectChunks(results)
}

type orTest struct {
	left, right BloomTest
}

// In addition to common `|= "foo" or "bar"`,
// orTest is particularly useful when testing skip-factors>0, which
// can result in different "sequences" of ngrams for a particular line
// and if either sequence matches the filter, the chunk is considered a match.
// For instance, with n=3,skip=1, the line "foobar" generates ngrams:
// ["foo", "oob", "oba", "bar"]
// Now let's say we want to search for the same "foobar".
// Note: we don't know which offset in the line this match may be,
// so we check every possible offset. The filter will match the ngrams:
// offset_0 creates ["foo", "oba"]
// offset_1 creates ["oob", "bar"]
// If either sequences are found in the bloom filter, the chunk is considered a match.
// Expanded, this is
// match == (("foo" && "oba") || ("oob" && "bar"))
func newOrTest(left, right BloomTest) orTest {
	return orTest{
		left:  left,
		right: right,
	}
}

func (o orTest) Matches(bloom *chkChecker) []*logproto.ChunkRef {
	var (
		jobs    = []BloomTest{o.left, o.right}
		results = make([][]*logproto.ChunkRef, 0, 2)
		lock    sync.Mutex
	)

	err := concurrency.ForEachJob(context.Background(), 2, 2, func(ctx context.Context, i int) error {
		rsp := jobs[i].Matches(bloom)
		lock.Lock()
		results = append(results, rsp)
		lock.Unlock()
		return nil
	})
	if err != nil {
		_ = level.Error(util_log.Logger).Log("msg", "error running orTest.Matches", "err", err)
		return bloom.refs
	}
	return unionChunks(results)
}

func newPatternTest(t Tokenizer, match string) BloomTest {
	lit, err := pattern.ParseLiterals(match)
	if err != nil {
		return MatchAll
	}

	var res BloomTests

	for _, l := range lit {
		res = append(res, newStringTest(t, string(l)))
	}
	return res
}
