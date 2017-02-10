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
	"strings"
)

type groupByGroup struct {
	fieldName  string
	fieldValue string
	metadata   map[string]string

	subGroupBy      []groupByGroup
	datapointGroups []datapointGroup
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
		for _, datapointGroupValues := range datapointGroups {
			for _, group := range datapointGroupValues {
				fieldValue := group.metadata[fieldName]
				fieldValues[fieldValue] = append(fieldValues[fieldValue], group)
			}
		}

		if subGroupByGroups == nil {
			subGroupByGroups = make([]groupByGroup, 0)
			for fieldValue, datapointGroups := range fieldValues {
				subGroupByGroups = append(subGroupByGroups, groupByGroup{
					fieldName:       fieldName,
					fieldValue:      fieldValue,
					datapointGroups: datapointGroups,
					metadata:        datapointGroups[0].metadata,
				})
			}
		} else {
			subGroupByGroups = make([]groupByGroup, 0)
			for fieldValue, _ := range fieldValues {
				subGroupByGroups = append(subGroupByGroups, groupByGroup{
					fieldName:  fieldName,
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

func renderTree(group groupByGroup, fieldsOrig map[string]string, keys map[string]struct{}, depth int) []map[string]string {
	// This should probably be severely optimized...
	fields := make(map[string]string)
	for k, v := range fieldsOrig {
		fields[k] = v
	}

	if group.fieldName != "__root" {
		fields[group.fieldName] = group.fieldValue
		keys[group.fieldName] = struct{}{}
	}
	if group.subGroupBy == nil {
		//for _,pointgroup := range group.datapointGroups {
		//fmt.Println(pointgroup.metadata)
		//}
		return []map[string]string{fields}
	}

	out := make([]map[string]string, 0)
	for _, subgroup := range group.subGroupBy {
		out = append(out, renderTree(subgroup, fields, keys, depth+1)...)
		//fmt.Println(out)
	}

	return out
}

func (g *groupByGroup) BuildResultSet() {
	keys := make(map[string]struct{}, 0)
	fields := make(map[string]string)

	rows := renderTree(*g, fields, keys, 0)
	keySlice := make([]string, 0)
	for key := range keys {
		keySlice = append(keySlice, key)
	}
	fmt.Println(strings.Join(keySlice, "\t\t"))
	for _, row := range rows {
		for _, key := range keySlice {
			fmt.Print(row[key], "\t")
		}
		fmt.Print("\n")
	}

}
