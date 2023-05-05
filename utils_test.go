package easykafka

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUnorderedStringsEqual(t *testing.T) {
	assert := assert.New(t)
	assert.True(unorderedStringsEqual([]string{"a", "b", "c"}, []string{"c", "b", "a"}))
	assert.False(unorderedStringsEqual([]string{"a", "b", "c"}, []string{"c", "b", "a", "d"}))
	assert.False(unorderedStringsEqual([]string{"a", "b", "c"}, []string{"c", "b"}))
	assert.False(unorderedStringsEqual([]string{"a", "b", "c"}, []string{"c", "b", "a", "d"}))
	assert.False(unorderedStringsEqual([]string{"a", "b", "c"}, []string{"c", "b", "a", "d"}))
	assert.False(unorderedStringsEqual([]string{"a", "b", "c"}, []string{"c", "b", "a", "d"}))
	assert.False(unorderedStringsEqual([]string{"a", "b", "c"}, []string{"c", "b", "a", "d"}))
	assert.False(unorderedStringsEqual([]string{"a", "b", "c"}, []string{"c", "b", "a", "d"}))
	assert.False(unorderedStringsEqual([]string{"a", "b", "c"}, []string{"c", "b", "a", "d"}))
}

// unit test easykafka.findMissing[T comparable](a, b []T) []T
func TestFindMissing(t *testing.T) {
	assert := assert.New(t)
	assert.Equal([]string{"a", "b"}, findMissing([]string{"a", "b", "c"}, []string{"c"}))
	assert.Equal([]string{"a", "b", "c"}, findMissing([]string{"a", "b", "c"}, []string{}))
	assert.Equal([]string{"a", "b", "c"}, findMissing([]string{"a", "b", "c"}, []string{"d"}))
	assert.Equal([]string{"a", "b", "c"}, findMissing([]string{"a", "b", "c"}, []string{"d", "e"}))
	assert.Equal([]string{"a", "b", "c"}, findMissing([]string{"a", "b", "c"}, []string{"d", "e", "f"}))
	assert.Equal([]string{"a", "b", "c"}, findMissing([]string{"a", "b", "c"}, []string{"d", "e", "f", "g"}))
	assert.Equal([]string{"a", "b", "c"}, findMissing([]string{"a", "b", "c"}, []string{"d", "e", "f", "g", "h"}))
	assert.Equal([]string{"a", "b", "c"}, findMissing([]string{"a", "b", "c"}, []string{"d", "e", "f", "g", "h", "i"}))
	assert.Equal([]string{"a", "b", "c"}, findMissing([]string{"a", "b", "c"}, []string{"d", "e", "f", "g", "h", "i", "j"}))
	assert.Equal([]string{"a", "b", "c"}, findMissing([]string{"a", "b", "c"}, []string{"d", "e", "f", "g", "h", "i", "j", "k"}))
	assert.Equal([]string{"a"}, findMissing([]string{"a"}, []string{"c", "d", "e"}))
	assert.Equal([]string{"a"}, findMissing([]string{"a", "b"}, []string{"b", "c", "d", "e"}))
	assert.Equal([]string{"a", "b"}, findMissing([]string{"a", "b", "c"}, []string{"c", "d", "e"}))
}

// unit test easykafka.convertSlice[T any](slice []*T) []T
func TestConvertSlice(t *testing.T) {
	assert := assert.New(t)
	assert.Equal([]string{"a", "b", "c"}, convertSlice([]*string{strP("a"), strP("b"), strP("c")}))
	assert.Equal([]int{1, 2, 3}, convertSlice([]*int{intP(1), intP(2), intP(3)}))
	assert.Equal([]string{"a", "b", "c"}, convertSlice([]*string{strP("a"), strP("b"), strP("c")}))
}

// unit test easykafka.appendIfMissing[T comparable](slice []T, elem T) []T
func TestAppendIfMissing(t *testing.T) {
	assert := assert.New(t)
	assert.Equal([]string{"a", "b", "c"}, appendIfMissing([]string{"a", "b"}, "c"))
	assert.Equal([]string{"a", "b", "c"}, appendIfMissing([]string{"a", "b", "c"}, "c"))
	assert.Equal([]string{"a", "b", "c"}, appendIfMissing([]string{"a", "b", "c"}, "c"))
	assert.Equal([]string{"a", "b", "c"}, appendIfMissing([]string{"a", "b", "c"}, "c"))
	assert.Equal([]string{"a", "b", "c"}, appendIfMissing([]string{"a", "b", "c"}, "c"))
	assert.Equal([]string{"a", "b", "c"}, appendIfMissing([]string{"a", "b", "c"}, "c"))
	assert.Equal([]string{"a", "b", "c"}, appendIfMissing([]string{"a", "b", "c"}, "c"))
	assert.Equal([]string{"a", "b", "c"}, appendIfMissing([]string{"a", "b", "c"}, "c"))
	assert.Equal([]string{"a", "b", "c"}, appendIfMissing([]string{"a", "b", "c"}, "c"))
}

// unit test easykafka.contains[T comparable](slice []T, elem T) bool
func TestContains(t *testing.T) {
	assert := assert.New(t)
	assert.True(contains([]string{"a", "b", "c"}, "a"))
	assert.True(contains([]string{"a", "b", "c"}, "b"))
	assert.True(contains([]string{"a", "b", "c"}, "c"))
	assert.False(contains([]string{"a", "b", "c"}, "d"))
	assert.False(contains([]string{"a", "b", "c"}, "e"))
	assert.False(contains([]string{"a", "b", "c"}, "f"))
	assert.False(contains([]string{"a", "b", "c"}, "g"))
	assert.False(contains([]string{"a", "b", "c"}, "h"))
	assert.False(contains([]string{"a", "b", "c"}, "i"))
	assert.False(contains([]string{"a", "b", "c"}, "j"))
	assert.False(contains([]string{"a", "b", "c"}, "k"))
}
