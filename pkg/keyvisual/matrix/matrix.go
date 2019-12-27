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

// Package matrix abstracts the source data as Plane, and then pixelates it into a matrix for display on the front end.
package matrix

import (
	"time"

	"github.com/pingcap/pd/pkg/keyvisual/decorator"
)

// Matrix is the front end displays the required data.
type Matrix struct {
	DataMap  map[string][][]uint64 `json:"data"`
	KeyAxis  []decorator.LabelKey  `json:"keyAxis"`
	TimeAxis []int64               `json:"timeAxis"`
}

// CreateMatrix uses the specified times and keys to build an initial matrix with no data.
func CreateMatrix(strategy Strategy, times []time.Time, keys []string, valuesListLen int) Matrix {
	dataMap := make(map[string][][]uint64, valuesListLen)
	// collect label keys
	keyAxis := make([]decorator.LabelKey, len(keys))
	for i, key := range keys {
		keyAxis[i] = strategy.Label(key)
	}
	// collect unix times
	timeAxis := make([]int64, len(times))
	for i, t := range times {
		timeAxis[i] = t.Unix()
	}
	return Matrix{
		DataMap:  dataMap,
		KeyAxis:  keyAxis,
		TimeAxis: timeAxis,
	}
}
