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
	"sladu/util/stop"
)

type Server struct {
	config  *Config
	stopper *stop.Stopper
}

func NewServer(config *Config, stopper *stop.Stopper) *Server {
	return &Server{
		config:  config,
		stopper: stopper,
	}
}

// TODO: Move all graphite stuff to its own Protocol package
func (s *Server) Start() error {
	err := graphite.NewServer(&s.config.Graphite, s.stopper).Start()
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) Stop() {
	// TODO: Abort all operations
}
