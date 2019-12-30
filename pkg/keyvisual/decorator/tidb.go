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
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/pd/pkg/apiutil/serverapi"
	"github.com/pingcap/pd/pkg/codec"
	"github.com/pingcap/pd/server"
)

type tableDetail struct {
	Name    string
	DB      string
	ID      int64
	Indices map[int64]string
}

type tidbLabelStrategy struct {
	tableMap    sync.Map
	tidbAddress []string

	svr   *server.Server
	ctx   context.Context
	group *server.APIGroup
}

// TiDBLabelStrategy implements the LabelStrategy interface. Get Label Information from TiDB.
func TiDBLabelStrategy(ctx context.Context, svr *server.Server, group *server.APIGroup, tidbAddress []string) LabelStrategy {
	return &tidbLabelStrategy{
		tidbAddress: tidbAddress,
		svr:         svr,
		ctx:         ctx,
		group:       group,
	}
}

func (s *tidbLabelStrategy) IsServiceAllowed() bool {
	if s.svr == nil {
		return true
	}
	return serverapi.IsServiceAllowed(s.svr, *s.group)
}

func (s *tidbLabelStrategy) Background() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if s.IsServiceAllowed() {
				s.updateAddress()
				s.updateMap()
			}
		}
	}
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
	} else if v, ok := s.tableMap.Load(TableID); ok {
		detail := v.(*tableDetail)
		label.Labels = append(label.Labels, detail.Name)
		if rowID := decodeKey.RowID(); rowID != 0 {
			label.Labels = append(label.Labels, fmt.Sprintf("row_%d", rowID))
		} else if indexID := decodeKey.IndexID(); indexID != 0 {
			label.Labels = append(label.Labels, detail.Indices[indexID])
		}
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
