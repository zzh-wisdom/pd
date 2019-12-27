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
	"os"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/keyvisual/storage"
	"github.com/pingcap/pd/server/core"
	"go.uber.org/zap"
)

type fileInput struct {
	startTime time.Time
	endTime   time.Time
	now       time.Time
}

// FileInput reads files in the specified time range from the ./data directory.
func FileInput(startTime, endTime time.Time) StatInput {
	return &fileInput{
		startTime: startTime,
		endTime:   endTime,
		now:       time.Now(),
	}
}

func (input *fileInput) GetStartTime() time.Time {
	return input.now.Add(input.startTime.Sub(input.endTime))
}

func (input *fileInput) Background(stat *storage.Stat) {
	log.Info("keyvisual load files from", zap.Time("start-time", input.startTime))
	fileTime := input.startTime
	for !fileTime.After(input.endTime) {
		regions := readFile(fileTime)
		fileTime = fileTime.Add(time.Minute)
		stat.Append(regions, input.now.Add(fileTime.Sub(input.endTime)))
	}
	log.Info("keyvisual load files to", zap.Time("end-time", input.endTime))
}

func readFile(fileTime time.Time) []*core.RegionInfo {
	fileName := fileTime.Format("./data/20060102-15-04.json")
	if file, err := os.Open(fileName); err == nil {
		return read(file)
	}
	return nil
}
