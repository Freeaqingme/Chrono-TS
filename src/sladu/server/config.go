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
package server

import (
	"sladu/protocol/graphite"
	"sladu/storage/mongo"
)

type Config struct {
	Graphite graphite.Config
	Mongo    mongo.Config
}

func NewConfig() *Config {
	return &Config{}
}
