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

package main

import (
	"context"
	"flag"
	"net/http"
	_ "net/http/pprof" //nolint:gosec
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/pd/pkg/keyvisual"
	"github.com/pingcap/pd/pkg/keyvisual/decorator"
	"github.com/pingcap/pd/pkg/keyvisual/input"
	"github.com/pingcap/pd/pkg/logutil"
	"github.com/pingcap/pd/pkg/ui"
	"github.com/pingcap/pd/server"
	"github.com/pkg/errors"
	"github.com/rs/cors"
	"go.uber.org/zap"
)

const (
	pdAPIPrefix = "/pd/"
	webPath     = "/web/"
)

// Config is the key visual server configuration.
type Config struct {
	*flag.FlagSet
	Version       bool
	Addr          string
	PDAddr        string
	TiDBAddr      string
	FileStartTime int64
	FileEndTime   int64
}

// NewConfig creates a new config.
func NewConfig() *Config {
	fs := flag.NewFlagSet("keyvisual", flag.ContinueOnError)
	cfg := &Config{
		FlagSet: fs,
	}
	fs.BoolVar(&cfg.Version, "V", false, "print version information and exit")
	fs.BoolVar(&cfg.Version, "version", false, "print version information and exit")
	fs.StringVar(&cfg.Addr, "addr", "0.0.0.0:32619", "address for listening")
	fs.StringVar(&cfg.PDAddr, "pd", "", "address for pd server in api mode")
	fs.StringVar(&cfg.TiDBAddr, "tidb", "", "address for tidb server")
	fs.Int64Var(&cfg.FileStartTime, "file-start", 0, "start time for file range in file mode")
	fs.Int64Var(&cfg.FileEndTime, "file-end", 0, "start time for file range in file mode")
	return cfg
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	if err := c.FlagSet.Parse(arguments); err != nil {
		return errors.WithStack(err)
	}
	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	apiMode := c.PDAddr != ""
	fileMode := c.FileStartTime > 0 || c.FileEndTime > 0
	if fileMode && (c.FileStartTime == 0 || c.FileEndTime == 0 || c.FileStartTime >= c.FileEndTime) {
		return errors.Errorf("%d ~ %d is an invalid time range", c.FileStartTime, c.FileEndTime)
	}
	if apiMode && fileMode {
		return errors.Errorf("'file' flag and 'pd' flag cannot exist at the same time")
	}
	if !apiMode && !fileMode {
		return errors.Errorf("'file' flag and 'pd' flag need to exist at least one")
	}

	return nil
}

// NewHandler creates a KeyvisualService.
func NewHandler(ctx context.Context, cfg *Config) http.Handler {
	var in input.StatInput
	var labelStrategy decorator.LabelStrategy

	if cfg.TiDBAddr == "" {
		labelStrategy = decorator.TiDBLabelStrategy(ctx, nil, nil, nil)
	} else {
		labelStrategy = decorator.TiDBLabelStrategy(ctx, nil, nil, []string{cfg.TiDBAddr})
	}

	if cfg.PDAddr != "" {
		in = input.APIInput(ctx, cfg.PDAddr)
	} else {
		startTime := time.Unix(cfg.FileStartTime, 0)
		endTime := time.Unix(cfg.FileEndTime, 0)
		in = input.FileInput(startTime, endTime)
	}

	return keyvisual.NewKeyvisualService(ctx, in, labelStrategy)
}

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])

	if cfg.Version {
		server.PrintPDInfo()
		exit(0)
	}

	defer logutil.LogPanic()

	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		exit(0)
	default:
		log.Fatal("parse cmd flags error", zap.Error(err))
	}

	// Flushing any buffered log entries
	defer log.Sync() //nolint:errcheck

	// Creates server.
	ctx, cancel := context.WithCancel(context.Background())
	// cors.Default() setup the middleware with default options being
	// all origins accepted with simple methods (GET, POST). See
	// documentation below for more options.
	apiHandler := cors.Default().Handler(NewHandler(ctx, cfg))
	webHandler := ui.Handler()
	http.Handle(pdAPIPrefix, apiHandler)
	http.Handle(webPath, http.StripPrefix(webPath, webHandler))

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	var sig os.Signal
	go func() {
		sig = <-sc
		cancel()
	}()

	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: http.DefaultServeMux,
	}

	log.Info("start key visual server")
	go srv.ListenAndServe() //nolint:errcheck

	<-ctx.Done()
	log.Info("Got signal to exit", zap.String("signal", sig.String()))
	if err := srv.Shutdown(ctx); err != nil {
		log.Error("Stop server", zap.Error(err))
	}

	switch sig {
	case syscall.SIGTERM:
		exit(0)
	default:
		exit(1)
	}
}

func exit(code int) {
	log.Sync() //nolint:errcheck
	os.Exit(code)
}
