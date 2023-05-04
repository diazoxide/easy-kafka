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
