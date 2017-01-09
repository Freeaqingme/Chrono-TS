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
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	//"log"
	"fmt"
	"sladu/storage"
	"strconv"
	"time"

	"github.com/spaolacci/murmur3"
)

const NR_REGULAR_VALUES = 512

// A bucket for regularly reoccuring metrics
// See also: https://www.mongodb.com/blog/post/schema-design-for-time-series-data-in-mongodb
type regularBucket struct {
	timestamp time.Time
	values    map[int]int
}

func (m *Mongo) persistMetric(metric storage.Metric) {
	granularity := int64(1)
	size := NR_REGULAR_VALUES * granularity
	offset := metric.Time().Unix() % size
	keyHash := murmur3.Sum32([]byte(metric.Key()))

	bucket := metric.Time().Unix() - offset - int64(keyHash)>>16

	change := mgo.Change{
		Update: bson.M{
			"$set": bson.M{"timestamp": metric.Time()},
			"$inc": bson.M{"values." + strconv.FormatInt(offset, 10): metric.Value()},
		},
		ReturnNew: true,
		Upsert:    true,
	}

	var doc regularBucket
	_, err := m.getDbConnection(keyHash).
		Find(bson.M{"_id": fmt.Sprintf("%s-%d-%d", metric.Key(), granularity, bucket)}).
		Apply(change, &doc)
	if err != nil {
		fmt.Println("Err", err.Error())
		return
	}
	//fmt.Println(doc, doc.timestamp, info, metric.Key())

}
