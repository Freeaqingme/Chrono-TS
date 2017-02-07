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
	"fmt"
	"log"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"

	"chronodium/server/tier"
	"chronodium/storage"
	"chronodium/util/stop"

	redis "gopkg.in/redis.v5"
)

const WORKERS = 48 // TODO: Make configurable

type Metric interface {
	Key() string
	Value() float64
	Time() time.Time
}

type Config struct{}

type Redis struct {
	config   *Config
	stopper  *stop.Stopper
	tierSets []*tier.TierSet

	sources map[string]<-chan storage.Metric
	client  redis.Cmdable
}

func NewRedis(config *Config, stopper *stop.Stopper, tierSets []*tier.TierSet) *Redis {
	out := &Redis{
		config:   config,
		stopper:  stopper,
		tierSets: tierSets,

		sources: make(map[string]<-chan storage.Metric, 0),
	}

	out.client = out.getNewClient()
	return out
}

func (r *Redis) AddSource(name string, src <-chan storage.Metric) {
	if _, exists := r.sources[name]; exists {
		panic("A source with name " + name + " already exists")
	}
	r.sources[name] = src
}

func (r *Redis) Start() {
	metrics := r.aggregateSources()

	for i := 0; i < WORKERS; i++ {
		go r.persistMetrics(metrics)
	}

	go r.persistRollups()
	go r.monitorSourceSizes(metrics)
}

func (r *Redis) getNewClient() redis.Cmdable {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func (r *Redis) monitorSourceSizes(metrics <-chan storage.Metric) {
	displaySize := func(name string, metric <-chan storage.Metric) {
		log.Printf("Queue %s has %d items", name, len(metric))
	}

	ticker := time.NewTicker(1 * time.Second)
	for _ = range ticker.C {
		log.Printf("Number of goroutines %d", runtime.NumGoroutine())
		displaySize("aggegrate", metrics)
		if len(metrics) == cap(metrics) {
			r.purgeQueuedMetrics(metrics)
		}

		for name, channel := range r.sources {
			displaySize(name, channel)
		}
	}
}

func (r *Redis) purgeQueuedMetrics(metrics <-chan storage.Metric) {
	i := 0
	for range metrics {
		i++
		if float64(len(metrics))*1.1 < float64(cap(metrics)) {
			log.Printf("Discarded %d metrics", i)
			return
		}
	}
}

func (r *Redis) aggregateSources() <-chan storage.Metric {
	var wg sync.WaitGroup
	out := make(chan storage.Metric, 1048560)

	output := func(c <-chan storage.Metric) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(r.sources))
	for _, c := range r.sources {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func (r *Redis) getGcKey(granularity int) string {
	return fmt.Sprintf("chronodium-%d-gc-%d", SCHEMA_VERSION, granularity)
}

func (r *Redis) getMetricNameFromRedisKey(key string) (metricName string, bucket, granularity int, error error) {
	// TODO compile me once
	regex := regexp.MustCompile("^chronodium-(?P<schemver>[0-9]+)-\\{metric-(?P<metric>.+)\\}-(?P<bucket>[0-9]{10})-(?P<granularity>[0-9]+)$")
	match := regex.FindStringSubmatch(key)
	if len(match) != 5 {
		return metricName, bucket, granularity, fmt.Errorf("Regex returned %d elements", len(match))
	}

	schemVer, _ := strconv.Atoi(match[1])
	if schemVer != SCHEMA_VERSION {
		return metricName, bucket, granularity, fmt.Errorf("Unsupported schema version: %s", match[0])
	}

	metricName = match[2]
	bucket, _ = strconv.Atoi(match[3])
	granularity, _ = strconv.Atoi(match[4])
	return
}

func (r *Redis) getNextTierForMetricAndGranularity(metricName string, granularity int) *tier.Tier {
	for _, v := range r.tierSets {
		if !v.Regex.MatchString(metricName) {
			continue
		}

		for k, tier := range v.Tiers {
			if int(tier.Granularity().Seconds()) != granularity {
				continue
			}
			if len(v.Tiers)-1 <= k {
				return nil
			}
			return v.Tiers[k+1]
		}
	}

	return nil
}
