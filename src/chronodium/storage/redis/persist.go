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
package redis

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"chronodium/storage"
	"chronodium/util/conversion"

	"github.com/twmb/murmur3"
	"gopkg.in/redis.v5"
)

const SCHEMA_VERSION = 1

const bucketWindow = 14400

func (r *Redis) persistMetrics(metrics <-chan storage.Metric) {
	ticker := time.NewTicker(1 * time.Second)

	client := r.getNewClient()
	pipeline := client.Pipeline()
	for metric := range metrics {
		r.persistMetric(pipeline, metric)

		select {
		case <-ticker.C:
			pipeline.Exec()
		default:
		}
	}
}

func (r *Redis) getBucket(shardKey string, timestamp *time.Time) int {
	keyHash := int(murmur3.Sum32([]byte(shardKey)))
	return int((timestamp.Unix()-int64(keyHash>>16))/bucketWindow) * bucketWindow
}

func (r *Redis) persistMetric(client *redis.Pipeline, metric storage.Metric) {
	metricTime := metric.Time()
	bucket := r.getBucket(metric.Key(), &metricTime)

	metadata := orderableMap(metric.Metadata()).ToJson()
	metadataHash := murmur3.Sum32(metadata)

	redisKey := fmt.Sprintf("chronodium-%d-{metric-%s}-%d-%d-raw-%d", SCHEMA_VERSION, metric.Key(), bucketWindow, bucket, metadataHash)

	buf := make([]byte, 16)
	conversion.Int64ToBinary(buf[0:8], metric.Time().UnixNano())
	conversion.Float64ToBinary(buf[8:16], metric.Value())
	client.Append(redisKey, string(buf))
	client.Expire(redisKey, 25*time.Hour)

	redisKey = fmt.Sprintf("chronodium-%d-{metric-%s}-%d-%d-raw", SCHEMA_VERSION, metric.Key(), bucketWindow, bucket)
	client.ZAdd(redisKey, redis.Z{float64(metadataHash), fmt.Sprintf("%d-%s", bucket, metadata)})
	client.Expire(redisKey, 25*time.Hour)
}

type orderableMap map[string]string

// See: http://stackoverflow.com/questions/25182923/go-golang-serialize-a-map-using-a-specific-order
func (om orderableMap) ToJson() []byte {
	var order []string
	for k := range om {
		order = append(order, k)
	}
	sort.Sort(sort.StringSlice(order))

	buf := &bytes.Buffer{}
	buf.Write([]byte{'{'})
	l := len(order)
	for i, k := range order {
		// Lets assume for now k nor om[k] contain quotes or backslashes
		fmt.Fprintf(buf, "\"%s\":\"%v\"", k, om[k])
		if i < l-1 {
			buf.WriteByte(',')
		}
	}
	buf.Write([]byte{'}'})
	return buf.Bytes()
}
