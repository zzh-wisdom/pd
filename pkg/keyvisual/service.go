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

	stat     *storage.Stat
	strategy matrix.Strategy
}

// NewKeyvisualService creates a HTTP handler for heatmaps service.
func NewKeyvisualService(ctx context.Context, in input.StatInput, labelStrategy decorator.LabelStrategy) *Service {
	strategy := matrix.DistanceStrategy(labelStrategy, math.Phi, 15)
	stat := storage.NewStat(defaultStatConfig, strategy, in.GetStartTime())
	go labelStrategy.Background()
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
func NewHandler(ctx context.Context, svr *server.Server) (http.Handler, server.APIGroup) {
	in := input.CoreInput(ctx, svr, defaultRegisterAPIGroupInfo)
	labelStrategy := decorator.TiDBLabelStrategy(ctx, svr, &defaultRegisterAPIGroupInfo, nil)
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
	startTime := endTime.Add(-1200 * time.Minute)

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
	baseTag := region.IntoTag(typ)
	plane := s.stat.Range(startTime, endTime, startKey, endKey, baseTag)
	resp := plane.Pixel(s.strategy, maxDisplayY, region.GetDisplayTags(baseTag))

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
