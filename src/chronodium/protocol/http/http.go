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
package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"chronodium/storage"
	"chronodium/storage/redis"
)

type httpServer struct {
	repo storage.Repo
}

func Start(repo storage.Repo) {
	s := &httpServer{
		repo: repo,
	}

	http.HandleFunc("/metrics/index.json",
		func(w http.ResponseWriter, r *http.Request) { s.graphiteHandler(w, r) })
	http.HandleFunc("/chrono-ts/query",
		func(w http.ResponseWriter, r *http.Request) { s.queryHandler(w, r) })
	go http.ListenAndServe(":8080", nil)
}

func (s *httpServer) queryHandler(w http.ResponseWriter, r *http.Request) {
	query := &storage.Query{Filter: make(map[string]string, 0), EndDate: time.Now(), StartDate: time.Now().Add(-1 * time.Hour)}
	query.ShardKey = r.URL.Query().Get("pk")
	if query.ShardKey == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("No primary key specified"))
		return
	}

	//var err error
	if startDate := r.URL.Query().Get("start-date"); startDate != "" {
		startDate, err := s.getTimeFromInput(startDate)
		query.StartDate = startDate
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Could not parse start-date: " + err.Error()))
			return
		}
	}

	if endDate := r.URL.Query().Get("end-date"); endDate != "" {
		endDate, err := s.getTimeFromInput(endDate)
		query.EndDate = endDate
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Could not parse end-date: " + err.Error()))
			return
		}
	}

	if filters, exists := r.URL.Query()["filter"]; exists {
		for _, filterString := range filters {
			parts := strings.Split(filterString, ":")
			if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Invalid filter specified: " + filterString))
				return
			}

			if _, exists := query.Filter[parts[0]]; exists {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Cannot use same filter key more than once: " + parts[0]))
				return
			}

			query.Filter[parts[0]] = parts[1]
		}
	}

	res := s.repo.Query(query).(redis.ResultSet)

	json.NewEncoder(w).Encode(
		struct {
			Results redis.ResultSet `json:"results"`
		}{Results: res},
	)
}

func (s *httpServer) getTimeFromInput(timeString string) (time.Time, error) {
	out, err := time.Parse(time.RFC3339, timeString)
	if err == nil {
		return out, nil
	}

	var timeInt int64
	timeInt, err = strconv.ParseInt(timeString, 10, 64)
	if err != nil {
		return out, err
	}

	return time.Unix(timeInt, 0), nil

}

func (s *httpServer) graphiteHandler(w http.ResponseWriter, r *http.Request) {
	metrics, _ := s.repo.GetMetricNames()

	if jsonp := r.URL.Query().Get("jsonp"); jsonp != "" {
		w.Header().Set("Content-Type", "application/javascript")

		w.Write([]byte(jsonp + "("))
		json.NewEncoder(w).Encode(metrics)
		w.Write([]byte(")"))
	} else {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(metrics)
	}
}
