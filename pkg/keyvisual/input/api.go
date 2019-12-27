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
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/pd/pkg/keyvisual/storage"
	"github.com/pingcap/pd/server/core"
)

type apiInput struct {
	addr string
	ctx  context.Context
}

// APIInput collects statistics from the PD API.
func APIInput(ctx context.Context, pdAddr string) StatInput {
	return &apiInput{
		addr: fmt.Sprintf("http://%s/pd/api/v1/regions", pdAddr),
		ctx:  ctx,
	}
}

func (input *apiInput) GetStartTime() time.Time {
	return time.Now()
}

func (input *apiInput) Background(stat *storage.Stat) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-input.ctx.Done():
			return
		case <-ticker.C:
			regions := regionRequest(input.addr)
			endTime := time.Now()
			stat.Append(regions, endTime)
		}
	}
}

func regionRequest(addr string) []*core.RegionInfo {
	if resp, err := http.Get(addr); err == nil { //nolint:bodyclose,gosec
		return read(resp.Body)
	}
	return nil
}
