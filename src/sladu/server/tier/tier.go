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
package tier

import (
	"fmt"
	"time"

	sladuTime "sladu/util/time"
)

const VALUES_PER_BUCKET = 128
const COLLECT_OFFSET = 30 * time.Second

type Tier struct {
	RawGranularity string `gcfg:"granularity"`
	RawTtl         string `gcfg:"ttl"`

	granularity   time.Duration
	ttl           time.Duration
	collectOffset float64
}

func (t *Tier) Granularity() time.Duration {
	return t.granularity
}

func (t *Tier) Ttl() time.Duration {
	return t.ttl
}

func (t *Tier) CollectOffset() float64 {
	return t.collectOffset
}

func (t *Tier) Validate() error {
	var err error
	if t.granularity, err = sladuTime.ParseDuration(t.RawGranularity); err != nil {
		return fmt.Errorf("Invalid Granularity '%s': %s", t.RawGranularity, err.Error())
	}

	if t.RawTtl == "" {
		t.ttl = t.calculateTtl()
	} else if t.ttl, err = sladuTime.ParseDuration(t.RawTtl); err != nil {
		return fmt.Errorf("Invalid TTL '%s': %s", t.RawTtl, err.Error())
	}

	t.collectOffset = float64(VALUES_PER_BUCKET*float64(t.granularity.Seconds())) + float64(COLLECT_OFFSET.Seconds())
	return nil
}

func (t *Tier) calculateTtl() time.Duration {
	return time.Duration(t.granularity.Seconds()*VALUES_PER_BUCKET) + (10 * COLLECT_OFFSET)
}
