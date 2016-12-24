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
package mongo

import (
	"fmt"
	"log"
	"sladu/storage"
	"sladu/util/stop"
	"sync"
	"time"
)

type Config struct{}

type Mongo struct {
	config  *Config
	stopper *stop.Stopper

	sources map[string]<-chan storage.Metric
}

func NewMongo(config *Config, stopper *stop.Stopper) *Mongo {
	return &Mongo{
		config:  config,
		stopper: stopper,
		sources: make(map[string]<-chan storage.Metric, 0),
	}
}

func (m *Mongo) AddSource(name string, src <-chan storage.Metric) {
	if _, exists := m.sources[name]; exists {
		panic("A source with name " + name + " already exists")
	}
	m.sources[name] = src
}

func (m *Mongo) Start() {
	metrics := m.aggregateSources()
	go m.run(metrics)
	go m.monitorSourceSizes(metrics)
}

func (m *Mongo) run(metrics <-chan storage.Metric) {
	for metric := range metrics {
		fmt.Println("Persisting", metric)
	}
}

func (m *Mongo) monitorSourceSizes(metrics <-chan storage.Metric) {
	displaySize := func(name string, metric <-chan storage.Metric) {
		log.Printf("Queue %s has %d items", name, len(metric))
	}

	ticker := time.NewTicker(5 * time.Second)
	for _ = range ticker.C {
		displaySize("aggegrate", metrics)
		for name, channel := range m.sources {
			displaySize(name, channel)
		}
	}
}

func (m *Mongo) aggregateSources() <-chan storage.Metric {
	var wg sync.WaitGroup
	out := make(chan storage.Metric, 1024)

	output := func(c <-chan storage.Metric) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(m.sources))
	for _, c := range m.sources {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
