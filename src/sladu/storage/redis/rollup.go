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
	"strconv"
	"time"

	"github.com/aristanetworks/goarista/monotime"
	"gopkg.in/redis.v5"
)

// TODO: Implement stopper?
func (r *Redis) persistRollups() {
	client := r.getNewClient()
	for _, granularity := range r.getGranularities() {
		go func(client redis.Cmdable, granularity int) {
			c := time.Tick(1 * time.Second)
			for range c {
				start := monotime.Now()
				amount := r.rollupGranularity(client, granularity)
				if amount > 0 {
					log.Printf("Rolled up %d items with granularity %d in %s",
						amount, granularity, monotime.Since(start))
				}
			}
		}(client, granularity)
	}
}

// TODO: Distributed locking
func (r *Redis) rollupGranularity(client redis.Cmdable, granularity int) int {
	zrangeArgs := redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(time.Now().Unix(), 10),
		Count: 4096,
	}
	gcKey := r.getGcKey(granularity)
	ret, err := client.ZRangeByScore(gcKey, zrangeArgs).Result()
	if err != nil {
		// TODO handle error
		fmt.Println(err)
	}

	pipe := client.Pipeline()
	for _, v := range ret {
		r.rollupBucket(pipe, v)
		pipe.ZRem(gcKey, v)
	}

	pipe.Exec()
	return len(ret)
}

type bucketValueTuple struct {
	value  float64
	bucket int64
}

func (r *Redis) rollupBucket(pipe *redis.Pipeline, bucketName string) {
	metrics, err := r.queryBucket(bucketName)
	if err != nil {
		// TODO handle error
		fmt.Println(err)
		return
	}

	metricName, _, granularity, err := r.getMetricNameFromRedisKey(bucketName)
	if err != nil {
		return // TODO error handling
	}

	nextTier := r.getNextTierForMetricAndGranularity(metricName, granularity)
	if nextTier == nil {
		return
	}

	buckets := make(map[string]map[int]bucketValueTuple, 0)
	for timestamp, measurement := range metrics {
		redisKey, bucket, offset := r.getBucketForMetric(metricName, int64(nextTier.Granularity().Seconds()), int64(timestamp))
		if _, ok := buckets[redisKey]; ok {
			bucketData := buckets[redisKey][offset]
			bucketData.value = bucketData.value + measurement
		} else {
			buckets[redisKey] = make(map[int]bucketValueTuple, 0)
			buckets[redisKey][offset] = bucketValueTuple{measurement, bucket}
		}
	}

	for redisKey, bucketData := range buckets {
		for offset, value := range bucketData {
			pipe.ZIncrBy(redisKey, value.value, strconv.Itoa(offset))
			pipe.ZAdd(r.getGcKey(int(nextTier.Granularity().Seconds())), redis.Z{float64(value.bucket) + nextTier.CollectOffset(), redisKey})
			pipe.Expire(redisKey, nextTier.Ttl())
		}
	}
}

func (r *Redis) getGranularities() []int {
	tmp := make(map[int]struct{})

	for _, tierSet := range r.tierSets {
		for _, tier := range tierSet.Tiers {
			tmp[int(tier.Granularity().Seconds())] = struct{}{}
		}
	}

	out := make([]int, 0)
	for granularity := range tmp {
		out = append(out, granularity)
	}

	return out
}
