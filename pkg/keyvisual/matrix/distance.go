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
	"context"
	"math"
	"runtime"
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

	SplitRatioPow []float64

	ScaleWorkers []chan *scaleTask
}

// DistanceStrategy adopts the strategy that the closer the split time is to the current time, the more traffic is
// allocated, when buckets are split.
// FIXME：返回的没必要是个接口类型吧，使得代码更加难懂
func DistanceStrategy(ctx context.Context, label decorator.LabelStrategy, ratio float64, level int, count int) Strategy {
	pow := make([]float64, level)
	for i := range pow {
		pow[i] = math.Pow(ratio, float64(i))
	}
	//matrix.DistanceStrategy(ctx, labelStrategy, 1.0/math.Phi, 15, 50)
	s := &distanceStrategy{
		LabelStrategy: label,
		SplitRatio:    ratio,
		SplitLevel:    level,
		SplitCount:    count,
		SplitRatioPow: pow,
		// 用于多线程的，workerCount是cpu的内核数
		ScaleWorkers:  make([]chan *scaleTask, workerCount),
	}
	//
	s.StartWorkers()
	go func() {
		<-ctx.Done()
		s.StopWorkers()
	}()
	return s
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
	// 初始值为轴的个数
	MemsetInt(virtualColumn, axesLen)

	// calculate left distance
	// 这样是假定最左边的轴chunks[0]离compactKeys最近吗？
	// dis的具体数值其实没啥用，只是用来辅助分层而已
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
	// 因此最后真正的dis是两者的最小值


	return distanceHelper{
		Scale: s.GenerateScale(chunks, compactKeys, dis),
	}
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
	// fixme:这里又把空接口转回来，不是多此一举吗
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

// multi-threaded calculate scale matrix.
var workerCount = runtime.NumCPU()

type scaleTask struct {
	*sync.WaitGroup
	Dis         []int
	Keys        []string
	CompactKeys []string
	Scale       *[]float64
}

func (s *distanceStrategy) StartWorkers() {
	for i := range s.ScaleWorkers {
		ch := make(chan *scaleTask)
		s.ScaleWorkers[i] = ch
		go s.GenerateScaleColumnWork(ch)
	}
}

func (s *distanceStrategy) StopWorkers() {
	for _, ch := range s.ScaleWorkers {
		ch <- nil
	}
}

func (s *distanceStrategy) GenerateScale(chunks []chunk, compactKeys []string, dis [][]int) [][]float64 {
	var wg sync.WaitGroup
	axesLen := len(chunks)
	scale := make([][]float64, axesLen)
	wg.Add(axesLen)
	for i := 0; i < axesLen; i++ {
		s.ScaleWorkers[i%workerCount] <- &scaleTask{
			WaitGroup:   &wg,
			Dis:         dis[i],
			Keys:        chunks[i].Keys,
			CompactKeys: compactKeys,
			Scale:       &scale[i],
		}
	}
	wg.Wait()
	return scale
}

func (s *distanceStrategy) GenerateScaleColumnWork(ch chan *scaleTask) {
	var maxDis int
	// Each split interval needs to be sorted after copying to tempDis
	var tempDis []int
	// Used as a mapping from distance to scale
	tempMapCap := 256
	tempMap := make([]float64, tempMapCap)

	for task := range ch {
		if task == nil {
			break
		}

		dis := task.Dis
		keys := task.Keys
		compactKeys := task.CompactKeys

		// The maximum distance between the StartKey and EndKey of a bucket
		// is considered the bucket distance.
		dis, maxDis = toBucketDis(dis)
		scale := make([]float64, len(dis))
		*task.Scale = scale

		// When it is not enough to accommodate maxDis, expand the capacity.
		for tempMapCap <= maxDis {
			tempMapCap *= 2
			// fixme:这代码～～
			tempMap = make([]float64, tempMapCap)
		}

/*<<<<<<< HEAD
		if start+1 == end {
			// 该bucket只分拆分成一份
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
=======*/
		// generate scale column
		start := 0
		// 找到第一个相等key的位置
		for startKey := keys[0]; !equal(compactKeys[start], startKey); {
			start++
		}
		end := start + 1

		for _, key := range keys[1:] {
			for !equal(compactKeys[end], key) {
				end++
			}

			if start+1 == end {
				// 未被分割的情况
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
						// 层数过大或者dis过小（太多）的，直接不分
						if level >= s.SplitLevel || i >= s.SplitCount {
							tempMap[d] = 0
						} else {
							// tempValue = math.Pow(s.SplitRatio, float64(level))
							// 具体的值已经和dis无关，只和level有关
							tempValue = s.SplitRatioPow[level]
							tempMap[d] = tempValue
						}
//>>>>>>> keyvis-dev
					}
					tempSum += tempValue
				}
				// Calculate scale
				// 距离越大分得越少
				for ; start < end; start++ {
					scale[start] = tempMap[dis[start]] / tempSum
				}
/*<<<<<<< HEAD
				tempSum += tempValue
			}
			// Calculate scale
			for ; start < end; start++ {
				// 真正计算所占的百分比
				scale[start] = tempMap[dis[start]] / tempSum
=======*/
//>>>>>>> keyvis-dev
			}
			end++
		}
		// task finish
		task.WaitGroup.Done()
	}
}

// dis的长度等于compactKeys的长度
// compactKeys应该是更细的轴
// updateLeftDis(dis[0], virtualColumn, chunks[0].Keys, compactKeys)
// 相当与计算key轴上每个分段在合并时，应该分给左边多少，dis就是具体参量
func updateLeftDis(dis, leftDis []int, keys, compactKeys []string) {
	CheckPartOf(compactKeys, keys)
	// j变量用来遍历短的原始keys
	j := 0
	//这个是比较短的
	keysLen := len(keys)
	// for i:=0;i<len(compactKeys);i++
	for i := range dis {
		if j < keysLen && equal(compactKeys[i], keys[j]) {
			dis[i] = 0
			j++
		} else {
			// 应该这样理解：计算左边的距离时，认为越靠近左边的value距离越小
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
