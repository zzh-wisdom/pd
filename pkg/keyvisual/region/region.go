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

// Package region defines the relationship between core.RegionInfo and Axis.
package region

import (
	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/keyvisual/matrix"
	"github.com/pingcap/pd/server/core"
	"go.uber.org/zap"
)

// Source data pre processing parameters.
const (
	// preThreshold   = 128
	// preRatioTarget = 512
	preTarget = 3072

	dirtyValue = 1 << 30
)

// StatTag is a tag for statistics of different dimensions.
type StatTag int

const (
	// Integration is The overall value of all other dimension statistics.
	Integration StatTag = iota
	// WrittenBytes is the size of the data written per minute.
	WrittenBytes
	// ReadBytes is the size of the data read per minute.
	ReadBytes
	// WrittenKeys is the number of keys written to the data per minute.
	WrittenKeys
	// ReadKeys is the number of keys read to the data per minute.
	ReadKeys
)

// IntoTag converts a string into a StatTag.
func IntoTag(typ string) StatTag {
	switch typ {
	case "":
		return Integration
	case "integration":
		return Integration
	case "written_bytes":
		return WrittenBytes
	case "read_bytes":
		return ReadBytes
	case "written_keys":
		return WrittenKeys
	case "read_keys":
		return ReadKeys
	default:
		return WrittenBytes
	}
}

func (tag StatTag) String() string {
	switch tag {
	case Integration:
		return "integration"
	case WrittenBytes:
		return "written_bytes"
	case ReadBytes:
		return "read_bytes"
	case WrittenKeys:
		return "written_keys"
	case ReadKeys:
		return "read_keys"
	default:
		panic("unreachable")
	}
}

// StorageTags is the order of tags during storage.
var StorageTags = []StatTag{WrittenBytes, ReadBytes, WrittenKeys, ReadKeys}

// ResponseTags is the order of tags when responding.
var ResponseTags = append([]StatTag{Integration}, StorageTags...)

// GetDisplayTags returns the actual order of the ResponseTags under the specified baseTag.
func GetDisplayTags(baseTag StatTag) []string {
	displayTags := make([]string, len(ResponseTags))
	for i, tag := range ResponseTags {
		displayTags[i] = tag.String()
		if tag == baseTag {
			displayTags[0], displayTags[i] = displayTags[i], displayTags[0]
		}
	}
	return displayTags
}

// Fixme: StartKey may not be equal to the EndKey of the previous region
func getKeys(regions []*core.RegionInfo) []string {
	keys := make([]string, len(regions)+1)
	keys[0] = core.String(regions[0].GetStartKey())
	endKeys := keys[1:]
	for i, region := range regions {
		endKeys[i] = core.String(region.GetEndKey())
	}
	return keys
}

func getValues(regions []*core.RegionInfo, tag StatTag) []uint64 {
	values := make([]uint64, len(regions))
	switch tag {
	case WrittenBytes:
		for i, region := range regions {
			values[i] = region.GetBytesWritten()
		}
	case ReadBytes:
		for i, region := range regions {
			values[i] = region.GetBytesRead()
		}
	case WrittenKeys:
		for i, region := range regions {
			values[i] = region.GetKeysWritten()
		}
	case ReadKeys:
		for i, region := range regions {
			values[i] = region.GetKeysRead()
		}
	case Integration:
		for i, region := range regions {
			values[i] = region.GetBytesWritten() + region.GetBytesRead()
		}
	default:
		panic("unreachable")
	}
	return values
}

// CreateStorageAxis converts the RegionInfo slice to a StorageAxis.
func CreateStorageAxis(regions []*core.RegionInfo, strategy matrix.Strategy) matrix.Axis {
	regionsLen := len(regions)
	if regionsLen <= 0 {
		panic("At least one RegionInfo")
	}

	keys := getKeys(regions)
	valuesList := make([][]uint64, len(ResponseTags))
	for i, tag := range ResponseTags {
		valuesList[i] = getValues(regions, tag)
	}

	preAxis := matrix.CreateAxis(keys, valuesList)
	wash(&preAxis)

	axis := IntoStorageAxis(preAxis, strategy)
	log.Info("New StorageAxis", zap.Int("region length", len(regions)), zap.Int("focus keys length", len(axis.Keys)))
	return axis
}

// IntoStorageAxis converts ResponseAxis to StorageAxis.
func IntoStorageAxis(responseAxis matrix.Axis, strategy matrix.Strategy) matrix.Axis {
	// axis := preAxis.Focus(strategy, preThreshold, len(keys)/preRatioTarget, preTarget)
	axis := responseAxis.Divide(strategy, preTarget)
	var storageValuesList [][]uint64
	// 把负载那一列的数据去掉
	storageValuesList = append(storageValuesList, axis.ValuesList[1:]...)
	return matrix.CreateAxis(axis.Keys, storageValuesList)
}

// IntoResponseAxis converts StorageAxis to ResponseAxis.
func IntoResponseAxis(storageAxis matrix.Axis, baseTag StatTag) matrix.Axis {
	// add integration values
	valuesList := make([][]uint64, 1, len(ResponseTags))
	writtenBytes := storageAxis.ValuesList[0]
	readBytes := storageAxis.ValuesList[1]
	integration := make([]uint64, len(writtenBytes))
	for i := range integration {
		integration[i] = writtenBytes[i] + readBytes[i]
	}
	valuesList[0] = integration
	valuesList = append(valuesList, storageAxis.ValuesList...)
	// swap baseTag
	for i, tag := range ResponseTags {
		if tag == baseTag {
			valuesList[0], valuesList[i] = valuesList[i], valuesList[0]
			return matrix.CreateAxis(storageAxis.Keys, valuesList)
		}
	}
	panic("unreachable")
}

// TODO: Temporary solution, need to trace the source of dirty data.
func wash(axis *matrix.Axis) {
	for i, value := range axis.ValuesList[0] {
		// 负载过高，直接置为0忽略
		if value >= dirtyValue {
			for j := range ResponseTags {
				axis.ValuesList[j][i] = 0
			}
		}
	}
}
