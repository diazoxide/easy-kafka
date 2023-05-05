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

	for _, v := range b {
		elements[v] = true
	}

	for _, v := range a {
		if _, ok := elements[v]; !ok {
			missing = append(missing, v)
		}
	}

	return missing
}

// unorderedStringsEqual compares two slices of strings without considering the order of the elements
func unorderedStringsEqual(a, b []string) bool {
	// Sort both slices
	sort.Strings(a)
	sort.Strings(b)

	// Compare the sorted slices
	return reflect.DeepEqual(a, b)
}

// function that convert slice of *any to slice of any
func convertSlice[T any](slice []*T) []T {
	var converted []T
	for _, v := range slice {
		converted = append(converted, *v)
	}
	return converted
}

// appendIfMissing appends elements to a slice if they are not already present
func appendIfMissing[T comparable](slice []T, s ...T) []T {
	for _, v := range s {
		if !contains(slice, v) {
			slice = append(slice, v)
		}
	}
	return slice
}

// contains checks if a slice contains an element
func contains[T comparable](slice []T, v T) bool {
	for _, e := range slice {
		if e == v {
			return true
		}
	}
	return false
}
