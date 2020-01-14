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

	"github.com/pingcap/pd/pkg/apiutil/serverapi"
	"github.com/pingcap/pd/pkg/keyvisual/storage"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/cluster"
	"github.com/pingcap/pd/server/core"
)

const limit = 1024

type coreInput struct {
	svr   *server.Server
	ctx   context.Context
	group server.APIGroup
}

// CoreInput collects statistics from the cluster.
func CoreInput(ctx context.Context, svr *server.Server, group server.APIGroup) StatInput {
	return &coreInput{
		svr:   svr,
		ctx:   ctx,
		group: group,
	}
}

func (input *coreInput) GetStartTime() time.Time {
	return time.Now()
}

// FIXME: works well with leader changed.
func (input *coreInput) Background(stat *storage.Stat) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-input.ctx.Done():
			return
		case <-ticker.C:
			rc := input.svr.GetRaftCluster()
			if rc != nil && serverapi.IsServiceAllowed(input.svr, input.group) {
				regions := clusterScan(input.ctx, rc)
				endTime := time.Now()
				stat.Append(regions, endTime)
			}
		}
	}
}

func clusterScan(ctx context.Context, rc *cluster.RaftCluster) []*core.RegionInfo {
	var startKey []byte
	endKey := []byte("")

	regions := make([]*core.RegionInfo, 0, limit)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		rs := rc.ScanRegions(startKey, endKey, limit)
		length := len(rs)
		if length == 0 {
			break
		}
		// 切片会自动扩容
		regions = append(regions, rs...)

		startKey = rs[length-1].GetEndKey()
		if len(startKey) == 0 {
			break
		}
	}

	// log.Info("Update key visual regions", zap.Int("total-length", len(regions)))
	return regions
}
