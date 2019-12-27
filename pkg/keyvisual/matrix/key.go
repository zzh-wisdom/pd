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

import (
	"reflect"
	"sync"
	"unsafe"
)

var keyMap sync.Map

// SaveKeys interns all strings.
// FIXME: GC useless keys
func SaveKeys(keys []string) {
	for i, key := range keys {
		uniqueKey, _ := keyMap.LoadOrStore(key, key)
		keys[i] = uniqueKey.(string)
	}
}

func equal(keyA, keyB string) bool {
	pA := (*reflect.StringHeader)(unsafe.Pointer(&keyA))
	pB := (*reflect.StringHeader)(unsafe.Pointer(&keyB))
	return pA.Data == pB.Data && pA.Len == pB.Len
}
