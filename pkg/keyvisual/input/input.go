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

// Package input defines several different data inputs.
package input

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"sort"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/pd/pkg/keyvisual/storage"
	"github.com/pingcap/pd/server/api"
	"github.com/pingcap/pd/server/core"
)

// StatInput is the interface that different data inputs need to implement.
type StatInput interface {
	GetStartTime() time.Time
	Background(stat *storage.Stat)
}

func read(stream io.ReadCloser) (regions []*core.RegionInfo) {
	defer stream.Close()
	var ars api.RegionsInfo
	decoder := json.NewDecoder(stream)
	if err := decoder.Decode(&ars); err == nil {
		sort.Slice(ars.Regions, func(i, j int) bool {
			return ars.Regions[i].StartKey < ars.Regions[j].StartKey
		})
		regions = make([]*core.RegionInfo, len(ars.Regions))
		for i, r := range ars.Regions {
			regions[i] = toCoreRegion(r)
		}
	}
	return
}

func toCoreRegion(ar *api.RegionInfo) *core.RegionInfo {
	startKey, _ := hex.DecodeString(ar.StartKey)
	endKey, _ := hex.DecodeString(ar.EndKey)
	meta := &metapb.Region{
		Id:          ar.ID,
		StartKey:    startKey,
		EndKey:      endKey,
		RegionEpoch: ar.RegionEpoch,
		Peers:       ar.Peers,
	}
	return core.NewRegionInfo(meta, ar.Leader,
		core.SetApproximateKeys(ar.ApproximateKeys),
		core.SetApproximateSize(ar.ApproximateSize),
		core.WithPendingPeers(ar.PendingPeers),
		core.WithDownPeers(ar.DownPeers),
		core.SetWrittenBytes(ar.WrittenBytes),
		core.SetWrittenKeys(ar.WrittenKeys),
		core.SetReadBytes(ar.ReadBytes),
		core.SetReadKeys(ar.ReadKeys),
	)
}
