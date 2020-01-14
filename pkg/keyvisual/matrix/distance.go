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
	"math"
	"sort"
	"sync"

	"github.com/pingcap/pd/pkg/keyvisual/decorator"
)

// TODO:
// * Multiplexing data between requests
// * Limit memory usage

type distanceHelper struct {
	Scale [][]float64
}

type distanceStrategy struct {
	decorator.LabelStrategy
	SplitRatio float64
	SplitLevel int
	SplitCount int
}

// DistanceStrategy adopts the strategy that the closer the split time is to the current time, the more traffic is
// allocated, when buckets are split.
func DistanceStrategy(label decorator.LabelStrategy, ratio float64, level int, count int) Strategy {
	return &distanceStrategy{
		SplitRatio:    1.0 / ratio,
		SplitLevel:    level,
		SplitCount:    count,
		LabelStrategy: label,
	}
}

func (s *distanceStrategy) GenerateHelper(chunks []chunk, compactKeys []string) interface{} {
	axesLen := len(chunks)
	keysLen := len(compactKeys)

	// generate key distance matrix
	dis := make([][]int, axesLen)
	for i := 0; i < axesLen; i++ {
		dis[i] = make([]int, keysLen)
	}

	// a column with the maximum value is virtualized on the right and left
	virtualColumn := make([]int, keysLen)
	MemsetInt(virtualColumn, axesLen)

	// calculate left distance
	// 这样是假定最左边的轴chunks[0]离compactKeys最近吗？
	updateLeftDis(dis[0], virtualColumn, chunks[0].Keys, compactKeys)
	for i := 1; i < axesLen; i++ {
		updateLeftDis(dis[i], dis[i-1], chunks[i].Keys, compactKeys)
	}
	// calculate the nearest distance on both sides
	// 这里应该是再假定最右边的轴chunks[end]离compactKeys最近吗，但计算距离时是取最小值
	end := axesLen - 1
	updateRightDis(dis[end], virtualColumn, chunks[end].Keys, compactKeys)
	for i := end - 1; i >= 0; i-- {
		updateRightDis(dis[i], dis[i+1], chunks[i].Keys, compactKeys)
	}

	// multi-threaded calculate scale matrix.
	// FIXME: Limit the number of concurrency
	var wg sync.WaitGroup
	scale := make([][]float64, axesLen)
	generateFunc := func(i int) {
		// The maximum distance between the StartKey and EndKey of a bucket
		// is considered the bucket distance.
		var maxDis int
		dis[i], maxDis = toBucketDis(dis[i])
		// The longer the distance, the lower the scale.
		scale[i] = s.GenerateScaleColumn(dis[i], maxDis, chunks[i].Keys, compactKeys)
		wg.Done()
	}
	wg.Add(axesLen)
	for i := 0; i < axesLen; i++ {
		go generateFunc(i)
	}
	wg.Wait()

	return distanceHelper{Scale: scale}
}

func (s *distanceStrategy) Split(dst, src chunk, tag splitTag, axesIndex int, helper interface{}) {
	CheckPartOf(dst.Keys, src.Keys)

	if len(dst.Keys) == len(src.Keys) {
		switch tag {
		case splitTo:
			copy(dst.Values, src.Values)
		case splitAdd:
			for i, v := range src.Values {
				dst.Values[i] += v
			}
		default:
			panic("unreachable")
		}
		return
	}

	start := 0
	for startKey := src.Keys[0]; !equal(dst.Keys[start], startKey); {
		start++
	}
	end := start + 1
	scale := helper.(distanceHelper).Scale

	switch tag {
	case splitTo:
		for i, key := range src.Keys[1:] {
			for !equal(dst.Keys[end], key) {
				end++
			}
			value := src.Values[i]
			for ; start < end; start++ {
				dst.Values[start] = uint64(float64(value) * scale[axesIndex][start])
			}
			end++
		}
	case splitAdd:
		for i, key := range src.Keys[1:] {
			for !equal(dst.Keys[end], key) {
				end++
			}
			value := src.Values[i]
			for ; start < end; start++ {
				dst.Values[start] += uint64(float64(value) * scale[axesIndex][start])
			}
			end++
		}
	default:
		panic("unreachable")
	}
}

func (s *distanceStrategy) GenerateScaleColumn(dis []int, maxDis int, keys, compactKeys []string) (scale []float64) {
	scale = make([]float64, len(dis))

	// Each split interval needs to be sorted after copying to tempDis
	var tempDis []int
	// Used as a mapping from distance to scale
	tempMap := make([]float64, maxDis+1)

	start := 0
	for startKey := keys[0]; !equal(compactKeys[start], startKey); {
		start++
	}
	end := start + 1

	for _, key := range keys[1:] {
		for !equal(compactKeys[end], key) {
			end++
		}

		if start+1 == end {
			// 改bucket只分拆分成一份
			// Optimize calculation when splitting into 1
			scale[start] = 1.0
			start++
		} else {
			// Copy tempDis and calculate the top n levels
			tempDis = append(tempDis[:0], dis[start:end]...)
			tempLen := len(tempDis)
			sort.Ints(tempDis)
			// Calculate distribution factors and sums based on distance ordering
			level := 0
			tempMap[tempDis[0]] = 1.0
			tempValue := 1.0
			tempSum := 1.0
			for i := 1; i < tempLen; i++ {
				d := tempDis[i]
				if d != tempDis[i-1] {
					level++
					// 这个啥意思哦？？？
					// 应该是满足这个条件的，直接不分配
					if level >= s.SplitLevel || i >= s.SplitCount {
						tempMap[d] = 0
					} else {
						// 直接根据level分配，与dis的数值并无关系了
						tempValue = math.Pow(s.SplitRatio, float64(level))
						tempMap[d] = tempValue
					}
				}
				tempSum += tempValue
			}
			// Calculate scale
			for ; start < end; start++ {
				// 真正计算所占的百分比
				scale[start] = tempMap[dis[start]] / tempSum
			}
		}
		end++
	}
	return
}

// dis的长度等于compactKeys的长度
// compactKeys应该是更细的轴
func updateLeftDis(dis, leftDis []int, keys, compactKeys []string) {
	CheckPartOf(compactKeys, keys)
	j := 0
	keysLen := len(keys)
	for i := range dis {
		if j < keysLen && equal(compactKeys[i], keys[j]) {
			dis[i] = 0
			j++
		} else {
			dis[i] = leftDis[i] + 1
		}
	}
}

func updateRightDis(dis, rightDis []int, keys, compactKeys []string) {
	j := 0
	keysLen := len(keys)
	for i := range dis {
		if j < keysLen && equal(compactKeys[i], keys[j]) {
			dis[i] = 0
			j++
		} else {
			dis[i] = Min(dis[i], rightDis[i]+1)
		}
	}
}

func toBucketDis(dis []int) ([]int, int) {
	maxDis := 0
	for i := len(dis) - 1; i > 0; i-- {
		dis[i] = Max(dis[i], dis[i-1])
		maxDis = Max(maxDis, dis[i])
	}
	return dis[1:], maxDis
}
