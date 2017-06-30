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
	"github.com/influxdata/influxdb/models"
	"time"
)

type metric struct {
	point models.Point
	value float64

	key  string
	tags map[string]string
}

func (m *metric) Value() float64 {
	return m.value
}

func (m *metric) Time() time.Time {
	return m.point.Time()
}

// TODO: Move to lua
func (m *metric) Key() string {
	if m.key == "" {
		if vhost, exists := m.Metadata()["vhost"]; exists {
			m.key = vhost + "__" + string(m.point.Name())
		} else {
			m.key = m.Metadata()["host"] + "__" + string(m.point.Name())
		}
	}

	return m.key
}

func (m *metric) Metadata() map[string]string {
	if m.tags == nil {
		m.tags = make(map[string]string, len(m.point.Tags())+1)
		m.tags["_key"] = string(m.point.Name())
		for _, tag := range m.point.Tags() {
			m.tags[string(tag.Key)] = string(tag.Value)
		}
	}

	return m.tags
}

func getMetricsFromInfluxPoint(point models.Point) []*metric {
	metrics := make([]*metric, 0)

	for _, p := range point.Split(1) {
		iter := p.FieldIterator()
		for iter.Next() { // Iterate over fields
			var value float64
			switch iter.Type() {
			case models.Integer:
				intVal, _ := iter.IntegerValue()
				value = float64(intVal)
			case models.Float:
				value, _ = iter.FloatValue()
			default:
				panic("Unsupported type in " + p.String())
			}

			m := &metric{
				point: p,
				value: value,
			}

			metrics = append(metrics, m)
		}
	}

	return metrics
}
