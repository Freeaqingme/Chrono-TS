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

	client := r.getClient()
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
	offset := (metric.Time().Unix() % (VALUES_PER_BUCKET * int64(t.Granularity().Seconds())))
	bucket := metric.Time().Unix() - offset
	timestamp := int(int(bucket) + int(float64(offset)/t.Granularity().Seconds()))

	keyHash := murmur3.Sum32([]byte(metric.Key()))
	bucketSkewed := bucket - int64(keyHash)>>16

	redisKey := fmt.Sprintf("sladu-%d-{metric-%s}-%d-%d", SCHEMA_VERSION, metric.Key(), bucketSkewed, granularity)
	client.ZIncrBy(redisKey, metric.Value(), strconv.Itoa(timestamp))
	client.ZAdd(fmt.Sprintf("sladu-%d-gc-%d", SCHEMA_VERSION, granularity), redis.Z{(float64(bucket) + t.CollectOffset()), redisKey})
	client.Expire(redisKey, t.Ttl())
}
