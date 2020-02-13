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

// Package keyvisual implements keyvisualService as the backend for the KeyVisualizer component, providing a visual web
// page to show key heatmap of the TiKV cluster, which is useful for troubleshooting and reasoning inefficient
// application usage patterns.
package keyvisual

import (
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/apiutil/serverapi"
	"github.com/pingcap/pd/pkg/keyvisual/decorator"
	"github.com/pingcap/pd/pkg/keyvisual/input"
	"github.com/pingcap/pd/pkg/keyvisual/matrix"
	"github.com/pingcap/pd/pkg/keyvisual/region"
	"github.com/pingcap/pd/pkg/keyvisual/storage"
	"github.com/pingcap/pd/server"
	"github.com/unrolled/render"
	"github.com/urfave/negroni"
	"go.uber.org/zap"
)

const (
	maxDisplayY = 1536
)

var (
	defaultStatConfig = storage.StatConfig{
		LayersConfig: []storage.LayerConfig{
			{Len: 60, Ratio: 2 / 1},                     // step 1 minutes, total 60, 1 hours (sum: 1 hours)
			{Len: 60 / 2 * 7, Ratio: 6 / 2},             // step 2 minutes, total 210, 7 hours (sum: 8 hours)
			{Len: 60 / 6 * 16, Ratio: 30 / 6},           // step 6 minutes, total 160, 16 hours (sum: 1 days)
			{Len: 60 / 30 * 24 * 6, Ratio: 4 * 60 / 30}, // step 30 minutes, total 288, 6 days (sum: 1 weeks)
			{Len: 24 / 4 * 28, Ratio: 0},                // step 4 hours, total 168, 4 weeks (sum: 5 weeks)
		},
	}

	defaultRegisterAPIGroupInfo = server.APIGroup{
		IsCore:  false,
		Name:    "keyvisual",
		Version: "v1",
	}
)

// Service is a HTTP handler for heatmaps service.
type Service struct {
	*http.ServeMux
	ctx context.Context
	rd  *render.Render

	stat     *storage.Stat   //分层存放数据
	strategy matrix.Strategy //生成矩阵的策略
}

// NewKeyvisualService creates a HTTP handler for heatmaps service.
func NewKeyvisualService(ctx context.Context, in input.StatInput, labelStrategy decorator.LabelStrategy) *Service {
	// 生成一个策略，这个策略只是合并的，不包括压缩的
	//strategy := matrix.DistanceStrategy(labelStrategy, math.Phi, 15, 50)
	// 生成一个分层队列
	//stat := storage.NewStat(defaultStatConfig, strategy, in.GetStartTime())
	// 开启线程不断维护label信息，从tidb中

	// 创建一个合并策略
	strategy := matrix.DistanceStrategy(ctx, labelStrategy, 1.0/math.Phi, 15, 50)
	// 创建分层存储队列
	stat := storage.NewStat(ctx, defaultStatConfig, strategy, in.GetStartTime())

	// 不断维护标签信息，也就是table信息
	go labelStrategy.Background()
	// 从pd中不断获取region信息
	go in.Background(stat)

	k := &Service{
		ServeMux: http.NewServeMux(),
		ctx:      ctx,
		rd:       render.New(render.Options{StreamingJSON: true}),
		stat:     stat,
		strategy: strategy,
	}

	k.HandleFunc("/pd/apis/keyvisual/v1/heatmaps", k.Heatmaps)
	return k
}

// NewHandler creates a KeyvisualService with CoreInput.
// main函数中会调用这个函数创建对应url的处理函数handler
func NewHandler(ctx context.Context, svr *server.Server) (http.Handler, server.APIGroup) {
	// 根据默认参数，创建StatInput输入接口， coreInput已经实现了这个接口
	in := input.CoreInput(ctx, svr, defaultRegisterAPIGroupInfo)
	// 创建标签生成策略，返回接口LabelStrategy，实际类型为tidbLabelStrategy
	labelStrategy := decorator.TiDBLabelStrategy(ctx, svr, &defaultRegisterAPIGroupInfo, nil)

	// 传入上面创建好实例
	// 新建Keyvisual服务（包含后台维护数据和热图生成）
	k := NewKeyvisualService(ctx, in, labelStrategy)

	handler := negroni.New(
		serverapi.NewRuntimeServiceValidator(svr, defaultRegisterAPIGroupInfo),
		serverapi.NewRedirector(svr),
		negroni.Wrap(k),
	)
	return handler, defaultRegisterAPIGroupInfo
}

// Heatmaps respond to a heatmap request.
func (s *Service) Heatmaps(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-type", "application/json")
	form := r.URL.Query()
	startKey := form.Get("startkey")
	endKey := form.Get("endkey")
	startTimeString := form.Get("starttime")
	endTimeString := form.Get("endtime")
	typ := form.Get("type")
	endTime := time.Now()
	startTime := endTime.Add(-360 * time.Minute)

	if startTimeString != "" {
		tsSec, err := strconv.ParseInt(startTimeString, 10, 64)
		if err != nil {
			log.Error("parse ts failed", zap.Error(err))
			s.rd.JSON(w, http.StatusBadRequest, "bad request")
			return
		}
		startTime = time.Unix(tsSec, 0)
	}
	if endTimeString != "" {
		tsSec, err := strconv.ParseInt(endTimeString, 10, 64)
		if err != nil {
			log.Error("parse ts failed", zap.Error(err))
			s.rd.JSON(w, http.StatusBadRequest, "bad request")
			return
		}
		endTime = time.Unix(tsSec, 0)
	}

	if !(startTime.Before(endTime) && (endKey == "" || startKey < endKey)) {
		s.rd.JSON(w, http.StatusBadRequest, "bad request")
		return
	}

	log.Info("Request matrix",
		zap.Time("start-time", startTime),
		zap.Time("end-time", endTime),
		zap.String("start-key", startKey),
		zap.String("end-key", endKey),
		zap.String("type", typ),
	)
	// 如果是""字符串，解码出来的仍然是""字符串
	if startKeyBytes, err := hex.DecodeString(startKey); err == nil {
		startKey = string(startKeyBytes)
	} else {
		s.rd.JSON(w, http.StatusBadRequest, "bad request")
		return
	}
	if endKeyBytes, err := hex.DecodeString(endKey); err == nil {
		endKey = string(endKeyBytes)
	} else {
		s.rd.JSON(w, http.StatusBadRequest, "bad request")
		return
	}
	// 默认是负载
	baseTag := region.IntoTag(typ)
	plane := s.stat.Range(startTime, endTime, startKey, endKey, baseTag)
	resp := plane.Pixel(s.strategy, maxDisplayY, region.GetDisplayTags(baseTag))
	resp.Range(startKey, endKey)
	// TODO: An expedient to reduce data transmission, which needs to be deleted later.
	resp.DataMap = map[string][][]uint64{
		// 去掉其余的数据，只保留typ类型的
		typ: resp.DataMap[typ],
	}

	// 可以返回json文本类型，也可以是json文本压缩成gzip的类型
	var encoder *json.Encoder
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer func() {
			if err := gz.Close(); err != nil {
				log.Warn("gzip close error", zap.Error(err))
			}
		}()
		encoder = json.NewEncoder(gz)
	} else {
		encoder = json.NewEncoder(w)
	}

	if err := encoder.Encode(resp); err != nil {
		log.Warn("json encode or write error", zap.Error(err))
	}
}
