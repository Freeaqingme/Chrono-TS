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
package redis

import (
	"fmt"
	"log"
	"sladu/storage"
	"strconv"
	"time"

	sladuTier "sladu/server/tier"

	"github.com/spaolacci/murmur3"
	"gopkg.in/redis.v5"
)

const VALUES_PER_BUCKET = 128

const SCHEMA_VERSION = 1

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

func (r *Redis) persistMetric(pipeline *redis.Pipeline, metric storage.Metric) {
	var tierSet *sladuTier.TierSet
	for _, v := range r.tierSets {
		if v.Regex.MatchString(metric.Key()) {
			tierSet = v
			break
		}
	}

	if tierSet == nil {
		log.Printf("NOTICE: No Tier Set found for metric %s", metric.Key())
		return
	}

	r.persistMetricInTier(pipeline, metric, tierSet.Tiers[0])
}

func (r *Redis) persistMetricInTier(client *redis.Pipeline, metric storage.Metric, t *sladuTier.Tier) {
	granularity := int(t.Granularity().Seconds())
	redisKey, bucket, timestamp := r.getBucketForMetric(metric.Key(), int64(granularity), metric.Time().Unix())
	gcTime := float64(bucket) + t.CollectOffset()

	client.ZIncrBy(redisKey, metric.Value(), strconv.Itoa(timestamp))
	client.ZAdd(r.getGcKey(granularity), redis.Z{gcTime, redisKey})
	client.ExpireAt(redisKey, time.Unix(int64(gcTime+t.Ttl().Seconds()), 0))
}

func (r *Redis) getBucketForMetric(metricName string, granularity int64, unixTime int64) (string, int64, int) {
	keyHash := int64(murmur3.Sum32([]byte(metricName)))

	offset := int64(unixTime) % (VALUES_PER_BUCKET * granularity)
	bucket := int64(unixTime) - offset + (keyHash >> 16 % (VALUES_PER_BUCKET * granularity))
	offset = offset - (offset % granularity)
	timestamp := int(int(unixTime) + int(float64(offset)))

	redisKey := fmt.Sprintf("sladu-%d-{metric-%s}-%d-%d", SCHEMA_VERSION, metricName, bucket, granularity)
	return redisKey, bucket, timestamp
}
