package easykafka

import (
	"reflect"
	"sort"
)

// findMissing[T comparable](a, b []T) []T
// Find the elements of the first slice that are not present in the second slice
func findMissing[T comparable](a, b []T) []T {
	var missing []T // Create a map to store the elements of the second slice
	elements := make(map[T]bool)

	// Add elements of the second slice to the map
	for _, v := range b {
		elements[v] = true
	}

	// Iterate over the first slice and check if the element exists in the map
	for _, v := range a {
		if _, ok := elements[v]; !ok {
			// Element is not found in the map (second slice), so it is missing
			missing = append(missing, v)
		}
	}

	return missing
}

func unorderedStringsEqual(a, b []string) bool {
	// Sort both slices
	sort.Strings(a)
	sort.Strings(b)

	// Compare the sorted slices
	return reflect.DeepEqual(a, b)
}
