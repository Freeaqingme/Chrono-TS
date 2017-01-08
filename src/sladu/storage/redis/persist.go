// Sladu - Keeping Time in Series
//
// Copyright 2016 Dolf Schimmel
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
	"sync"
	"sladu/storage"
	"strconv"

	sladuTier "sladu/server/tier"

	"github.com/spaolacci/murmur3"
	"gopkg.in/redis.v3"
)

const VALUES_PER_BUCKET = 128

const SCHEMA_VERSION = 1

func (r *Redis) persistMetric(metric storage.Metric) {
	pipeline := r.client.Pipeline()

	wg := sync.WaitGroup{}
	wg.Add(len(r.tiers))
	for _, tier := range r.tiers {
		go func(tier *sladuTier.Tier) {
			r.persistMetricInTier(pipeline, metric, tier)
			wg.Done()
		}(tier)
	}

	wg.Wait()
	pipeline.Exec()
}

func (r *Redis) persistMetricInTier(client *redis.Pipeline, metric storage.Metric, t *sladuTier.Tier) {
	granularity := int(t.Granularity().Seconds())
	offset := (metric.Time().Unix() % (VALUES_PER_BUCKET * int64(t.Granularity().Seconds())))
	bucket := metric.Time().Unix() - offset
	bucketOffset := int(float64(offset) / t.Granularity().Seconds())

	keyHash := murmur3.Sum32([]byte(metric.Key()))
	bucketSkewed := bucket - int64(keyHash)>>16

	redisKey := fmt.Sprintf("sladu-%d-{metric-%s}-%d-%d", SCHEMA_VERSION, metric.Key(), bucketSkewed, granularity)
	client.ZIncrBy(redisKey, metric.Value(), strconv.Itoa(bucketOffset))
	client.ZAdd(fmt.Sprintf("sladu-%d-gc-%d", SCHEMA_VERSION, granularity), redis.Z{(float64(bucket) + t.CollectOffset()), redisKey})
	client.Expire(redisKey, t.Ttl())
}
