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
package tier

import (
	"fmt"
	"regexp"
	"sort"
)

type orderableTierSet []*TierSet

type TierSet struct {
	RawTiers []string `gcfg:"tier"`
	Match    string

	Order int
	Regex *regexp.Regexp
	Tiers []*Tier
	Id    string
}

func (t *TierSet) Validate(tiers map[string]*Tier) error {
	var err error
	t.Regex, err = regexp.Compile(t.Match)

	if err != nil {
		return fmt.Errorf("Could not parse regex %s: %s", t.Match, err.Error())
	}

	if len(t.RawTiers) == 0 {
		return fmt.Errorf("No Tiers have been defined")
	}

	t.Tiers = make([]*Tier, len(t.RawTiers))
	prevGranularity := 0.0
	for k, v := range t.RawTiers {
		var ok bool
		if t.Tiers[k], ok = tiers[v]; !ok {
			return fmt.Errorf("Unknown tier referenced: %s", v)
		}

		if t.Tiers[k].granularity.Seconds() <= prevGranularity {
			// This is not necessarily technically wrong, but it makes no sense
			// and is indicative of human error.
			return fmt.Errorf("The Granularity of Tier '%s' is lower or equal to the previous one", v)
		}
		prevGranularity = t.Tiers[k].granularity.Seconds()
	}

	return nil
}

func GetOrderedTierSets(tiers map[string]*TierSet) []*TierSet {
	out := make([]*TierSet, len(tiers))
	i := 0
	for k, v := range tiers {
		v.Id = k
		out[i] = v
		i++
	}

	sort.Sort(orderableTierSet(out))
	return out
}

func (s orderableTierSet) Len() int {
	return len(s)
}
func (s orderableTierSet) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s orderableTierSet) Less(i, j int) bool {
	return s[i].Order < s[j].Order
}
