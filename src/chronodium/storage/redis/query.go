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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"chronodium/storage"
	"sort"
	"strconv"
)

const (
	timeFmt = "2006-01-02T15:04:05.999999999"
)

func (r *Redis) Query(query *storage.Query) storage.ResultSet {
	//startTime := time.Now().Add(-24 * time.Hour)
	//endTime := time.Now()
	/*	filter := map[string]string{
		"host": "dolf-ThinkPad-T460s",
		//"instance": "enp0s31f6",
		//"type": "if_octets",
	}*/

	datapointGroups := make(map[int][]datapointGroup, 0)

	buckets, _ := r.getBucketsInWindow(*query.GetStartDate(), *query.GetEndDate(), query.ShardKey)
	for _, bucket := range buckets {
		groups := r.queryBucket(query.ShardKey, bucket, query.Filter)
		for _, group := range groups {
			datapointGroups[group.metadataHash] = append(datapointGroups[group.metadataHash], *group)
		}
	}

	out := make(ResultSet, 0)
	for _, dpgs := range datapointGroups {
		for _, dpg := range dpgs {
			for _, point := range dpg.points {
				//entry["_value"] = strconv.FormatFloat(point.value, 'e', -1, 64)
				//entry["_timestamp"] = time.Unix(0, point.timestamp).Format("2006-01-02T15:04:05.999999999")
				//fmt.Println(dpg.metadata)
				//out = append(out, entry)
				point.metadata = dpg.metadata
				out = append(out, point)
			}
		}
	}

	sort.Sort(out)
	//fmt.Println(out)
	return out

}

type ResultSet []*datapoint

func (p ResultSet) Len() int {
	return len(p)
}

func (p ResultSet) Less(i, j int) bool {
	return p[i].timestamp < (p[j].timestamp)
}

func (p ResultSet) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p *datapoint) MarshalJSON() ([]byte, error) {
	out := make(map[string]string, len(p.metadata)+2)
	for k, v := range p.metadata {
		out[k] = v
	}

	out["_date"] = time.Unix(0, p.timestamp).Format(timeFmt)
	out["_value"] = strconv.FormatFloat(p.value, 'e', -1, 64)
	return json.Marshal(out)

}

func (r *Redis) queryBucket(shardKey string, bucket int, filter map[string]string) []*datapointGroup {
	out := make([]*datapointGroup, 0)

	metadataHashes := r.getFilteredMetadataHashes(shardKey, bucket, filter)
	for hash, metadata := range metadataHashes {
		redisKey := fmt.Sprintf("chronodium-%d-{metric-%s}-%d-%d-raw-%d", SCHEMA_VERSION, shardKey, 14400, bucket, hash)
		rawPoints, err := r.client.Get(redisKey).Bytes()
		if err != nil {
			log.Println("Error from Redis: ", err.Error())
			return out
		}

		out = append(out, &datapointGroup{r.unpackPoints(rawPoints), metadata, hash})
	}

	return out
}

func (r *Redis) unpackPoints(rawPoints []byte) []*datapoint {
	out := make([]*datapoint, 0, len(rawPoints)/16)
	buf := bytes.NewBuffer(rawPoints)

	var timestamp int64
	var value float64

	length := len(rawPoints)
	for i := 0; i < length; i = i + 16 {
		binary.Read(buf, binary.LittleEndian, &timestamp)
		binary.Read(buf, binary.LittleEndian, &value)

		out = append(out, &datapoint{timestamp, value, nil})
	}

	return out
}

func (r *Redis) getFilteredMetadataHashes(shardKey string, bucket int, filter map[string]string) map[int]map[string]string {
	redisKey := fmt.Sprintf("chronodium-%d-{metric-%s}-%d-%d-raw", SCHEMA_VERSION, shardKey, 14400, bucket)
	res, _ := r.client.ZRangeWithScores(redisKey, 0, -1).Result()

	metadataHashes := make(map[int]map[string]string, 0)
RowLoop:
	for _, z := range res {
		hash := int(z.Score)

		metadata := make(map[string]string, 0)
		err := json.Unmarshal([]byte(z.Member.(string)), &metadata)
		if err != nil {
			log.Println("Error unmarshalling json: ", err.Error())
			continue
		}

		for k, v := range filter {
			if metadataValue, ok := metadata[k]; !ok || metadataValue != v {
				continue RowLoop
			}
		}

		metadataHashes[hash] = metadata
	}

	return metadataHashes
}

func (r *Redis) getBucketsInWindow(startTime, endTime time.Time, shardKey string) ([]int, error) {
	buckets := make([]int, 0)

	if startTime.After(endTime) {
		return buckets, fmt.Errorf("Start time must be smaller than or equal to end time")
	}

	for !startTime.After(endTime) {
		buckets = append(buckets, r.getBucket(shardKey, &startTime))
		startTime = startTime.Add(14400 * time.Second)
	}

	return buckets, nil
}

func (r *Redis) GetMetricNames() (metricNames []string, err error) {
	return []string{}, nil
}

type datapoint struct {
	timestamp int64
	value     float64
	metadata  map[string]string
}

type datapointGroup struct {
	points       []*datapoint
	metadata     map[string]string
	metadataHash int
}
