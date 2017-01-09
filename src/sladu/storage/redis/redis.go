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
	"log"
	"runtime"
	"sync"
	"time"

	"sladu/server/tier"
	"sladu/storage"
	"sladu/util/stop"

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
}

func NewRedis(config *Config, stopper *stop.Stopper, tierSets []*tier.TierSet) *Redis {
	return &Redis{
		config:   config,
		stopper:  stopper,
		tierSets: tierSets,
		sources:  make(map[string]<-chan storage.Metric, 0),
	}
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

	go r.monitorSourceSizes(metrics)

	// TODO defer session.Close()
}

func (r *Redis) getClient() *redis.Client {
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
