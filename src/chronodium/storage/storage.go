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
package storage

import "time"

type Metric interface {
	Key() string
	Value() float64
	Time() time.Time
	Metadata() map[string]string
}

type Repo interface {
	GetMetricNames() (metricNames []string, err error)
	Query(*Query) ResultSet
}

type ResultSet interface {
}

type Query struct {
	ShardKey  string
	StartDate *time.Time
	EndDate   *time.Time
	Filter    map[string]string
}

func (q *Query) GetStartDate() *time.Time {
	if q.StartDate == nil {
		startDate := time.Now().Add(-1 * time.Hour)
		q.StartDate = &startDate
	}

	return q.StartDate
}

func (q *Query) GetEndDate() *time.Time {
	if q.EndDate == nil {
		endDate := time.Now()
		q.EndDate = &endDate
	}

	return q.EndDate
}
