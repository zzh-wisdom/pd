// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package matrix

import "sort"

// MemsetUint64 sets all elements of the uint64 slice to v.
func MemsetUint64(slice []uint64, v uint64) {
	sliceLen := len(slice)
	if sliceLen == 0 {
		return
	}
	slice[0] = v
	for bp := 1; bp < sliceLen; bp <<= 1 {
		copy(slice[bp:], slice[:bp])
	}
}

// MemsetInt sets all elements of the int slice to v.
func MemsetInt(slice []int, v int) {
	sliceLen := len(slice)
	if sliceLen == 0 {
		return
	}
	slice[0] = v
	for bp := 1; bp < sliceLen; bp <<= 1 {
		copy(slice[bp:], slice[:bp])
	}
}

// GetLastKey gets the last element of keys.
func GetLastKey(keys []string) string {
	return keys[len(keys)-1]
}

// CheckPartOf checks that part keys are a subset of src keys.
func CheckPartOf(src, part []string) {
	err := src[0] > part[0] || len(src) < len(part)
	srcLastKey := GetLastKey(src)
	partLastKey := GetLastKey(part)
	if srcLastKey != "" && (partLastKey == "" || srcLastKey < partLastKey) {
		err = true
	}
	if err {
		panic("The inclusion relationship is not satisfied between keys")
	}
}

// CheckReduceOf checks that part keys are a subset of src keys and have the same StartKey and EndKey.
func CheckReduceOf(src, part []string) {
	if src[0] != part[0] || GetLastKey(src) != GetLastKey(part) || len(src) < len(part) {
		panic("The inclusion relationship is not satisfied between keys")
	}
}

// MakeKeys uses a key map to build a key. If unlimitedEnd is true, MakeKeys need to add a "" to the keys, indicating
// that the last bucket has an unlimited end.
func MakeKeys(keySet map[string]struct{}, unlimitedEnd bool) []string {
	keysLen := len(keySet)
	keys := make([]string, keysLen, keysLen+1)
	i := 0
	for key := range keySet {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	if unlimitedEnd {
		keys = append(keys, "")
	}
	return keys
}

// Max returns the larger of a and b.
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Min returns the smaller of a and b.
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
