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
	"time"

	"bytes"
	"chronodium/storage"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"gopkg.in/redis.v5"
	"log"
	"strings"
)

var queryClient redis.Cmdable

const (
	AggrSum = iota
)

func (r *Redis) Query(query *storage.Query) {
	startTime := time.Now().Add(-24 * time.Hour)
	endTime := time.Now()
	filter := map[string]string{
		"host": "dolf-ThinkPad-T460s",
	}
	groupBy := []string{"instance", "type"}
	//groupBy := []string{ "instance"}
	if len(groupBy) > 1 {
		log.Println("Grouping by more than one instance is flaky at best!")
	}
	//aggr := AggrSum

	datapointGroups := make(map[int][]datapointGroup, 0)

	buckets, _ := r.getBucketsInWindow(startTime, endTime, query.ShardKey)
	fmt.Println(buckets)
	//return

	for _, bucket := range buckets {
		groups := r.queryBucket(query.ShardKey, bucket, filter)
		fmt.Println(bucket, groups)
		//continue
		for _, group := range groups {
			var group2 datapointGroup
			group2 = *group
			if _, exists := datapointGroups[group2.metadataHash]; exists {
				datapointGroups[group2.metadataHash] = append(datapointGroups[group2.metadataHash], *group)
			} else {
				datapointGroups[group2.metadataHash] = []datapointGroup{group2}
			}
		}
	}
	//fmt.Println("datapointGroups")
	//for k , v := range datapointGroups {
	//	fmt.Println(k, v[0].metadataHash, v[0].metadata)
	//fmt.Println(k, v[1].metadataHash, v[1].metadata)
	//}

	//fmt.Println("Lenght:::::", len(datapointGroups))
	grouped := newGroupByGroup(groupBy, datapointGroups)
	//for _, groups := range datapointGroups {
	//	fmt.Println(hash, groups[0].metadata)
	//for _, group := range groups {
	//	for _, point := range group.points{
	//		fmt.Printf("%d, %.4f\n", point.timestamp, point.value, group.metadata)
	//	}
	//}
	//}

	fmt.Println("tree")
	showTree(grouped, 0)

}

func showTree(group groupByGroup, depth int) {
	fmt.Println(depth, strings.Repeat("    ", depth), group.fieldName, "\t", group.fieldValue, "\t", len(group.subGroupBy), group.metadata)
	if group.subGroupBy == nil {
		//for _,pointgroup := range group.datapointGroups {
		//fmt.Println(pointgroup.metadata)
		//}
		return
	}

	for _, subgroup := range group.subGroupBy {
		showTree(subgroup, depth+1)
	}
}

func newGroupByGroup(groupBy []string, datapointGroups map[int][]datapointGroup) groupByGroup {
	root := groupByGroup{
		fieldName:       "__root",
		fieldValue:      "__root",
		subGroupBy:      make([]groupByGroup, 0),
		datapointGroups: make([]datapointGroup, 0),
	}

	if len(groupBy) == 0 {
		for _, groups := range datapointGroups {
			for _, group := range groups {
				root.datapointGroups = append(root.datapointGroups, group)
			}
		}
		return root
	}

	var subGroupByGroups []groupByGroup
	for _, fieldName := range groupBy {
		parentGroupByGroups := subGroupByGroups
		fieldValues := make(map[string][]datapointGroup, 0)

		if subGroupByGroups == nil {
			for _, datapointGroupValues := range datapointGroups {
				//fmt.Println("116", datapointGroupValues) // , datapointGroupValues[0].metadata)
				for _, group := range datapointGroupValues {

					var group2 datapointGroup
					group2 = group
					fieldValue := (group2.metadata)[fieldName]
					//fmt.Println("118", fieldValue, group.metadata)
					if _, exists := fieldValues[fieldValue]; !exists {
						fieldValues[fieldValue] = make([]datapointGroup, 0)
					}
					fieldValues[fieldValue] = append(fieldValues[fieldValue], group2)
				}
			}

			subGroupByGroups = make([]groupByGroup, 0)
			for fieldValue, datapointGroups := range fieldValues {
				fmt.Println("fieldValue", fieldValue, datapointGroups[0].metadata)
				subGroupByGroups = append(subGroupByGroups, groupByGroup{
					fieldName:       fieldName,
					fieldValue:      fieldValue,
					datapointGroups: datapointGroups,
					metadata:        datapointGroups[0].metadata,
				})
			}
		} else {
			fieldValues := make(map[string][]groupByGroup, 0)
			for _, subGroup := range subGroupByGroups {
				fieldValue := subGroup.metadata[fieldName]
				if _, exists := fieldValues[fieldValue]; !exists {
					fieldValues[fieldValue] = make([]groupByGroup, 0)
				}
				fmt.Println(fieldValue, subGroup.metadata)
				fieldValues[fieldValue] = append(fieldValues[fieldValue], subGroup)
			}

			subGroupByGroups = make([]groupByGroup, 0)
			for fieldValue, datapointGroups := range fieldValues {
				subGroupByGroups = append(subGroupByGroups, groupByGroup{
					fieldName:  fieldName,
					fieldValue: fieldValue,
					subGroupBy: parentGroupByGroups,
					metadata:   datapointGroups[0].metadata,
				})
			}

		}
	}

	root.subGroupBy = subGroupByGroups
	return root
}

type groupByGroup struct {
	fieldName  string
	fieldValue string

	metadata  	map[string]string // Get rid of me
	metadataHashes []int

	subGroupBy      []groupByGroup
	datapointGroups []datapointGroup
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

		out = append(out, &datapoint{timestamp, value})
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
}

type datapointGroup struct {
	points       []*datapoint
	metadata     map[string]string
	metadataHash int
}
