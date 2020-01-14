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
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pingcap/log"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	retryCnt                  = 10
	etcdGetTimeout            = time.Second
	tidbServerInformationPath = "/tidb/server/info"
)

type serverInfo struct {
	ID         string `json:"ddl_id"`
	IP         string `json:"ip"`
	Port       uint   `json:"listening_port"`
	StatusPort uint   `json:"status_port"`
}

type dbInfo struct {
	Name struct {
		O string `json:"O"`
		L string `json:"L"`
	} `json:"db_name"`
	State int `json:"state"`
}

type tableInfo struct {
	ID   int64 `json:"id"`
	Name struct {
		O string `json:"O"`
		L string `json:"L"`
	} `json:"name"`
	Indices []struct {
		ID   int64 `json:"id"`
		Name struct {
			O string `json:"O"`
			L string `json:"L"`
		} `json:"idx_name"`
	} `json:"index_info"`
}

// 这个函数就是不断更新（替换的形式）tidb的地址，
func (s *tidbLabelStrategy) updateAddress() {
	if s.svr == nil {
		return
	}
	cli := s.svr.GetClient()
	var info serverInfo
	for i := 0; i < retryCnt; i++ {
		var tidbAddress []string
		// 这里不太懂啊，慢慢学吧
		ectx, cancel := context.WithTimeout(s.ctx, etcdGetTimeout)
		resp, err := cli.Get(ectx, tidbServerInformationPath, clientv3.WithPrefix())
		cancel()
		if err != nil {
			log.Warn("get key failed", zap.String("key", tidbServerInformationPath), zap.Error(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, kv := range resp.Kvs {
			err = json.Unmarshal(kv.Value, &info)
			if err != nil {
				log.Warn("get key failed", zap.String("key", tidbServerInformationPath), zap.Error(err))
				continue
			}
			tidbAddress = append(tidbAddress, fmt.Sprintf("%s:%d", info.IP, info.StatusPort))
		}
		if len(tidbAddress) > 0 {
			s.tidbAddress = tidbAddress
		}
	}
}

func (s *tidbLabelStrategy) updateMap() {
	var dbInfos []*dbInfo
	var tidbAddr string
	for _, addr := range s.tidbAddress {
		// 只要有一个tidb回应就可以了
		if err := request(addr, "schema", &dbInfos); err == nil {
			tidbAddr = addr
			break
		}
	}
	if dbInfos == nil {
		return
	}

	var tableInfos []*tableInfo
	for _, db := range dbInfos {
		if db.State == 0 {
			continue
		}
		if err := request(tidbAddr, fmt.Sprintf("schema/%s", db.Name.O), &tableInfos); err != nil {
			continue
		}
		for _, table := range tableInfos {
			indices := make(map[int64]string, len(table.Indices))
			for _, index := range table.Indices {
				indices[index.ID] = index.Name.O
			}
			detail := &tableDetail{
				Name:    table.Name.O,
				DB:      db.Name.O,
				ID:      table.ID,
				Indices: indices,
			}
			s.tableMap.Store(table.ID, detail)
		}
	}
}

func request(addr string, uri string, v interface{}) error {
	url := fmt.Sprintf("http://%s/%s", addr, uri)
	resp, err := http.Get(url) //nolint:gosec
	if err != nil {
		log.Warn("request failed", zap.String("url", url))
		return err
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	return decoder.Decode(v)
}
