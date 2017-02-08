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
	"strconv"

	"fmt"
	"gopkg.in/redis.v5"
)

var queryClient redis.Cmdable

func (r *Redis) queryBucket(bucket string) (map[int]float64, error) {
	out := make(map[int]float64, 0)

	ret, err := r.client.ZRangeWithScores(bucket, 0, -1).Result()
	if err != nil {
		return out, err
	}

	for _, v := range ret {
		timestamp, _ := strconv.Atoi(v.Member.(string))
		out[timestamp] = v.Score
	}

	return out, nil
}

func (r *Redis) GetMetricNames() ([]string, error) {
	keys := make(map[string]struct{})
	client := r.getNewClient()

	match := fmt.Sprintf("chronodium-%d-{metric-*}-*", SCHEMA_VERSION)
	iterator := client.Scan(0, match, 4096).Iterator()
	for {
		if !iterator.Next() {
			break
		}

		// TODO
		//metric, _, _, _ := r.getMetricNameFromRedisKey(iterator.Val())
		//keys[metric] = struct{}{}
		if !iterator.Next() { // Skip over score
			break
		}
	}

	out := make([]string, 0, len(keys))
	for key := range keys {
		out = append(out, key)
	}
	return out, nil
}
