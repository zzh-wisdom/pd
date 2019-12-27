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

package input

import (
	"context"
	"time"

	"github.com/pingcap/pd/pkg/keyvisual/storage"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/cluster"
	"github.com/pingcap/pd/server/core"
)

type coreInput struct {
	svr *server.Server
	ctx context.Context
}

// CoreInput collects statistics from the cluster.
func CoreInput(ctx context.Context, svr *server.Server) StatInput {
	return &coreInput{
		svr: svr,
		ctx: ctx,
	}
}

func (input *coreInput) GetStartTime() time.Time {
	return time.Now()
}

func (input *coreInput) Background(stat *storage.Stat) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-input.ctx.Done():
			return
		case <-ticker.C:
			rc := input.svr.GetRaftCluster()
			if rc == nil {
				continue
			}
			regions := clusterScan(rc)
			endTime := time.Now()
			stat.Append(regions, endTime)
		}
	}
}

func clusterScan(rc *cluster.RaftCluster) []*core.RegionInfo {
	var startKey []byte
	endKey := []byte("")

	regions := make([]*core.RegionInfo, 0, 1024)

	for {
		rs := rc.ScanRegions(startKey, endKey, 1024)
		length := len(rs)
		if length == 0 {
			break
		}

		regions = append(regions, rs...)

		startKey = rs[length-1].GetEndKey()
		if len(startKey) == 0 {
			break
		}
	}

	// log.Info("Update key visual regions", zap.Int("total-length", len(regions)))
	return regions
}
