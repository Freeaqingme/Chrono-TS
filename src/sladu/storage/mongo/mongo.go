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
package mongo

import (
	//"fmt"
	"fmt"
	"gopkg.in/mgo.v2"
	"log"
	"sladu/storage"
	"sladu/util/stop"
	"strconv"
	"sync"
	"time"
)

const SCHEMA_VERSION = 1
const WORKERS = 16 // TODO: Make configurable
const DATABASES = 8

type Config struct{}

type Mongo struct {
	config   *Config
	stopper  *stop.Stopper
	cRegular []*mgo.Collection

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
	m.connect()

	metrics := m.aggregateSources()

	for i := 0; i < WORKERS; i++ {
		go m.persistMetrics(metrics)
	}

	go m.monitorSourceSizes(metrics)

	fmt.Println(mgo.GetStats())

	// TODO defer session.Close()
}

func (m *Mongo) connect() {
	m.cRegular = make([]*mgo.Collection, DATABASES)
	for i := int64(0); i < DATABASES; i++ {
		session, err := mgo.Dial("localohst,localhost") // Todo: Make configurable
		if err != nil {
			panic(err)
		}

		session.SetMode(mgo.Eventual, true) // Todo: Make configurable

		m.cRegular[i] = session.DB("sladu_" + strconv.FormatInt(i, 10)).C("regular_v" + strconv.Itoa(SCHEMA_VERSION))
	}

	mgo.SetStats(true)
	go func() {
		for {
			time.Sleep(10 * time.Second)
			fmt.Println(mgo.GetStats())
		}
	}()

}

func (m *Mongo) getDbConnection(keyHash uint32) *mgo.Collection {
	return m.cRegular[keyHash%DATABASES]
}

func (m *Mongo) persistMetrics(metrics <-chan storage.Metric) {
	for metric := range metrics {
		//fmt.Println("Persisting", metric)
		m.persistMetric(metric)
	}
}

func (m *Mongo) monitorSourceSizes(metrics <-chan storage.Metric) {
	displaySize := func(name string, metric <-chan storage.Metric) {
		log.Printf("Queue %s has %d items", name, len(metric))
	}

	ticker := time.NewTicker(1 * time.Second)
	for _ = range ticker.C {
		displaySize("aggegrate", metrics)
		if len(metrics) == cap(metrics) {
			m.purgeQueuedMetrics(metrics)
		}

		for name, channel := range m.sources {
			displaySize(name, channel)
		}
	}
}

func (m *Mongo) purgeQueuedMetrics(metrics <-chan storage.Metric) {
	i := 0
	for range metrics {
		i++
		if float64(len(metrics))*1.1 < float64(cap(metrics)) {
			log.Printf("Discarded %d metrics", i)
			return
		}
	}
}

func (m *Mongo) aggregateSources() <-chan storage.Metric {
	var wg sync.WaitGroup
	out := make(chan storage.Metric, 1048560)

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
