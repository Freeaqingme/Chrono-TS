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
	"strconv"
	"strings"
	"time"
)

type groupByGroup struct {
	fieldName  string
	fieldValue string
	metadata   map[string]string

	subGroupBy      []groupByGroup
	datapointGroups []datapointGroup
}

func newGroupByGroup(groupers []grouper, datapointGroups map[int][]datapointGroup) groupByGroup {
	root := groupByGroup{
		fieldName:       "__root",
		fieldValue:      "__root",
		subGroupBy:      make([]groupByGroup, 0),
		datapointGroups: make([]datapointGroup, 0),
	}

	if len(groupers) == 0 {
		for _, groups := range datapointGroups {
			for _, group := range groups {
				root.datapointGroups = append(root.datapointGroups, group)
			}
		}
		return root
	}

	var subGroupByGroups []groupByGroup
	for _, grouper := range groupers {
		parentGroupByGroups := subGroupByGroups
		fieldValues := make(map[string][]datapointGroup, 0)
		for _, datapointGroupValues := range datapointGroups {
			for _, group := range datapointGroupValues {
				for fieldValue, splitGroup := range grouper.GroupByValue(group) {
					fieldValues[fieldValue] = append(fieldValues[fieldValue], splitGroup)
				}
			}
		}

		if subGroupByGroups == nil {
			subGroupByGroups = make([]groupByGroup, 0)
			for fieldValue, datapointGroups := range fieldValues {
				subGroupByGroups = append(subGroupByGroups, groupByGroup{
					fieldName:       grouper.Key(),
					fieldValue:      fieldValue,
					datapointGroups: datapointGroups,
					metadata:        datapointGroups[0].metadata,
				})
			}
		} else {
			subGroupByGroups = make([]groupByGroup, 0)
			for fieldValue, _ := range fieldValues {
				subGroupByGroups = append(subGroupByGroups, groupByGroup{
					fieldName:  grouper.Key(),
					fieldValue: fieldValue,
					subGroupBy: parentGroupByGroups,
				})
			}

		}
	}

	root.subGroupBy = subGroupByGroups
	return root
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

func (g *groupByGroup) buildResultSet(fieldsOrig map[string]groupByGroup, keys map[string]struct{}, depth int) []map[string]groupByGroup {
	// This should probably be severely optimized...
	fields := make(map[string]groupByGroup)
	for k, v := range fieldsOrig {
		fields[k] = v
	}

	if g.fieldName != "__root" {
		fields[g.fieldName] = *g // g.fieldValue
		keys[g.fieldName] = struct{}{}
	}
	if g.subGroupBy == nil {
		//for _,pointgroup := range g.datapointGroups {
		//fmt.Println(pointgroup.metadata)
		//}
		return []map[string]groupByGroup{fields}
	}

	out := make([]map[string]groupByGroup, 0)
	for _, subgroup := range g.subGroupBy {
		out = append(out, subgroup.buildResultSet(fields, keys, depth+1)...)
	}

	return out
}

func (g *groupByGroup) BuildResultSet(renderKeys []string, groupers []grouper) {
	keys := make(map[string]struct{}, 0)
	fields := make(map[string]groupByGroup)

	rows := g.buildResultSet(fields, keys, 0)

	if len(renderKeys) == 0 {
		for key := range keys {
			renderKeys = append(renderKeys, key)
		}
	}

	renderKeysGroup := make(map[string]struct{}, 0)
	renderKeysNoGroup := make(map[string]struct{}, 0)

separateGroupedKeys:
	for _, renderKey := range renderKeys {
		for _, grouper := range groupers {
			if grouper.Key() == renderKey {
				renderKeysGroup[renderKey] = struct{}{}
				continue separateGroupedKeys
			}
		}
		renderKeysNoGroup[renderKey] = struct{}{}
	}

	fmt.Println(strings.Join(renderKeys, "\t\t"))
	for _, row := range rows {
		for key := range renderKeysGroup {
			fmt.Printf("%s:%s\t", key, row[key].fieldValue)
		}
		mostSpecificGroup := row[groupers[len(groupers)-1].Key()]
		for key := range renderKeysNoGroup {
			fmt.Printf("%s:%s\t", key, mostSpecificGroup.getDataPointGroups()[0].metadata[key])
		}
		fmt.Print("\n")
	}

}

func (g *groupByGroup) getDataPointGroups() []datapointGroup {
	if g.datapointGroups != nil {
		return g.datapointGroups
	}

	out := make([]datapointGroup, 0)
	for _, subGroup := range g.subGroupBy {
		out = append(out, subGroup.getDataPointGroups()...)
	}

	return out
}

type grouper interface {
	Key() string
	GroupByValue(datapointGroup) map[string]datapointGroup
}

type groupByString struct {
	key string
}

func (g *groupByString) GroupByValue(dpg datapointGroup) map[string]datapointGroup {
	out := make(map[string]datapointGroup, 0)
	out[dpg.metadata[g.Key()]] = dpg
	return out
}

func (g *groupByString) Key() string {
	return g.key
}

func newGroupByStringer(key string) *groupByString {
	return &groupByString{key}
}

type groupByTime struct {
	window time.Duration
}

func (g *groupByTime) Key() string {
	return "__time_" + strconv.FormatInt(int64(g.window), 10)
}

func (g *groupByTime) GroupByValue(dpg datapointGroup) map[string]datapointGroup {
	out := make(map[string]datapointGroup, 0)
	for _, point := range dpg.points {
		bucket := strconv.Itoa(int((point.timestamp / int64(g.window)) * int64(g.window)))
		newPointGroup, exists := out[bucket]
		if !exists {
			newPointGroup = datapointGroup{
				points:       make([]*datapoint, 0),
				metadata:     dpg.metadata,
				metadataHash: dpg.metadataHash,
			}
			out[bucket] = newPointGroup
		}
		newPointGroup.points = append(out[bucket].points, point)
	}

	return out
}

func newGroupByTime(window time.Duration) *groupByTime {
	return &groupByTime{window}
}
