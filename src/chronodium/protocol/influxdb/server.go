// Chronodium - Keeping Time in Series
//
// Copyright 2016-2017 Dolf Schimmel
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package influxdb

import (
	"net"
	"net/http"
	"strconv"
	"time"

	"chronodium/storage"
	"chronodium/util/stop"

	"bytes"
	"fmt"
	"github.com/influxdata/influxdb/models"
)

type Config struct {
	Enable bool
	Port   int
	Bind   net.IP
}

type Server struct {
	config  *Config
	stopper *stop.Stopper
	storage chan storage.Metric
}

func NewServer(config *Config, stopper *stop.Stopper) *Server {
	return &Server{
		config:  config,
		stopper: stopper,
		storage: make(chan storage.Metric, 1024),
	}
}

func (s *Server) Start() error {
	http.HandleFunc("/write",
		func(w http.ResponseWriter, r *http.Request) { s.writeHandler(w, r) })
	go http.ListenAndServe(s.config.Bind.String()+":"+strconv.Itoa(s.config.Port), nil)

	return nil
}

func (s *Server) writeHandler(w http.ResponseWriter, r *http.Request) {
	var bs []byte
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	points, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), r.URL.Query().Get("precision"))
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	for _, point := range points {
		for _, m := range getMetricsFromInfluxPoint(point) {
			//fmt.Println(m.Time(), m.Key(), m.Value(), m.Tags())
			s.storage <- m
		}

	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) Metrics() <-chan storage.Metric {
	return s.storage
}
