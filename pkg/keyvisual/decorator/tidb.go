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

package decorator

import (
	"encoding/hex"
	"fmt"

	"github.com/pingcap/pd/pkg/codec"
)

type tidbLabelStrategy struct {
}

// TiDBLabelStrategy implements the LabelStrategy interface. Get Label Information from TiDB.
func TiDBLabelStrategy() LabelStrategy {
	return &tidbLabelStrategy{}
}

func (s *tidbLabelStrategy) Background() {
}

// CrossBorder does not allow cross tables or cross indexes within a table.
func (s *tidbLabelStrategy) CrossBorder(startKey, endKey string) bool {
	startBytes, endBytes := codec.Key(Bytes(startKey)), codec.Key(Bytes(endKey))
	startIsMeta, startTableID := startBytes.MetaOrTable()
	endIsMeta, endTableID := endBytes.MetaOrTable()
	if startIsMeta || endIsMeta {
		return startIsMeta != endIsMeta
	}
	if startTableID != endTableID {
		return true
	}
	startIndex := startBytes.IndexID()
	endIndex := endBytes.IndexID()
	return startIndex != endIndex
}

// Label will parse the ID information of the table and index.
// Fixme: the label information should get from tidb.
func (s *tidbLabelStrategy) Label(key string) (label LabelKey) {
	keyBytes := Bytes(key)
	label.Key = hex.EncodeToString(keyBytes)
	decodeKey := codec.Key(keyBytes)
	isMeta, TableID := decodeKey.MetaOrTable()
	if isMeta {
		label.Labels = append(label.Labels, "meta")
	} else {
		label.Labels = append(label.Labels, fmt.Sprintf("table_%d", TableID))
		if rowID := decodeKey.RowID(); rowID != 0 {
			label.Labels = append(label.Labels, fmt.Sprintf("row_%d", rowID))
		} else if indexID := decodeKey.IndexID(); indexID != 0 {
			label.Labels = append(label.Labels, fmt.Sprintf("index_%d", indexID))
		}
	}
	return
}
