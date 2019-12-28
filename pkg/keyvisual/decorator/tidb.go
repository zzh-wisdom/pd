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
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/codec"
	"github.com/pingcap/pd/server"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// AddressCrawler returns the address of tidb
type AddressCrawler func() []string

const (
	retryCnt                  = 10
	etcdGetTimeout            = time.Second
	tidbServerInformationPath = "/tidb/server/info"
)

// LocalServerCrawler gets the latest tidb address list from the local pd server.
var LocalServerCrawler = func(ctx context.Context, svr *server.Server) AddressCrawler {
	cli := svr.GetClient()
	return func() []string {
		for i := 0; i < retryCnt; i++ {
			var res []string
			ectx, cancel := context.WithTimeout(ctx, etcdGetTimeout)
			resp, err := cli.Get(ectx, tidbServerInformationPath, clientv3.WithPrefix())
			cancel()
			if err != nil {
				log.Warn("get key failed", zap.String("key", tidbServerInformationPath), zap.Error(err))
				time.Sleep(200 * time.Millisecond)
				continue
			}
			for _, kv := range resp.Kvs {
				v := make(map[string]interface{})
				err = json.Unmarshal(kv.Value, &v)
				if err != nil {
					log.Warn("get key failed", zap.String("key", tidbServerInformationPath), zap.Error(err))
					continue
				}
				res = append(res, v["ip"].(string))
			}
			return res
		}
		return nil
	}
}

type tidbLabelStrategy struct {
	addrCrawler AddressCrawler
}

// TiDBLabelStrategy implements the LabelStrategy interface. Get Label Information from TiDB.
func TiDBLabelStrategy(c AddressCrawler) LabelStrategy {
	return &tidbLabelStrategy{
		addrCrawler: c,
	}
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
