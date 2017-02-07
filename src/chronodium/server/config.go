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
package server

import (
	"fmt"

	"chronodium/protocol/graphite"
	"chronodium/protocol/influxdb"
	"chronodium/server/tier"
	"chronodium/storage/redis"
)

type Config struct {
	Graphite graphite.Config
	Influxdb influxdb.Config
	Redis    redis.Config

	Tiers            map[string]*tier.Tier    `gcfg:"tier"`
	UnorderedTierSet map[string]*tier.TierSet `gcfg:"tier-set"`
	TierSets         []*tier.TierSet
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) Validate() error {
	for k, v := range c.Tiers {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("Error parsing Tier '%s': %s", k, err.Error())
		}
	}

	for k, v := range c.UnorderedTierSet {
		if err := v.Validate(c.Tiers); err != nil {
			return fmt.Errorf("Error parsing Tier Set '%s': %s", k, err.Error())
		}
	}

	c.TierSets = tier.GetOrderedTierSets(c.UnorderedTierSet)

	return nil
}
