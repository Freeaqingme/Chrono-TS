// Sladu - Keeping Time in Series
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
package graphite

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type metric struct {
	key   string
	value float64
	ts    time.Time
}

func (m *metric) Key() string {
	return m.key
}

func (m *metric) Value() float64 {
	return m.value
}

func (m *metric) Time() time.Time {
	return m.ts
}

func (s *Server) processRawMetric(line string) error {
	m, err := s.parseLine(line)
	if err != nil {
		return err
	}

	s.storage <- m
	return nil
}

func (s *Server) parseLine(line string) (m *metric, err error) {
	parts := strings.SplitN(line, " ", 4)
	if len(parts) != 3 {
		return nil, fmt.Errorf("Found %d parts, expected 3", len(parts))
	}

	m = &metric{
		key: parts[0],
	}
	if m.value, err = strconv.ParseFloat(string(parts[1]), 64); err != nil {
		return nil, err
	}

	i, err := strconv.ParseInt(strings.TrimSpace(string(parts[2])), 10, 64)
	if err != nil {
		return nil, err
	}
	m.ts = time.Unix(i, 0)

	return m, nil
}
