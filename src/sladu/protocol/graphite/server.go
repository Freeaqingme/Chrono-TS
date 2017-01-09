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
package graphite

import (
	"io"
	"log"
	"net"
	"strconv"
	"time"

	"bufio"
	"sladu/storage"
	"sladu/util/stop"
)

type Config struct {
	Enable bool
	Port   int
	Bind   net.IP
}

type Server struct {
	config  *Config
	stopper *stop.Stopper
	storage chan storage.Metric
}

func NewServer(config *Config, stopper *stop.Stopper) *Server {
	return &Server{
		config:  config,
		stopper: stopper,
		storage: make(chan storage.Metric, 1024),
	}
}

func (s *Server) Start() error {
	laddr, err := net.ResolveTCPAddr("tcp", s.config.Bind.String()+":"+strconv.Itoa(s.config.Port))
	if nil != err {
		return err
	}
	listener, err := net.ListenTCP("tcp", laddr)
	if nil != err {
		return err
	}
	log.Printf("listening on %s", listener.Addr())

	go s.Serve(listener)
	return nil
}

func (s *Server) Serve(listener *net.TCPListener) {
	for {
		select {
		case <-s.stopper.ShouldStop():
			log.Println("stopping listening on", listener.Addr())
			listener.Close()
			return
		default:
		}
		listener.SetDeadline(time.Now().Add(1e9))
		conn, err := listener.AcceptTCP()
		if nil != err {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			log.Println(err)
			continue
		}
		//		log.Println(conn.RemoteAddr(), "connected")
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn *net.TCPConn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		select {
		case <-s.stopper.ShouldStop():
			log.Println("disconnecting because of application stop: ", conn.RemoteAddr())
			return
		default:
		}
		conn.SetDeadline(time.Now().Add(1e12))
		message, err := reader.ReadString('\n')
		if err == io.EOF {
			return
		} else if err != nil {
			log.Println(err.Error())
			return
		}

		if err := s.processRawMetric(message); err != nil {
			log.Println(err.Error())
		}
	}
}

func (s *Server) Metrics() <-chan storage.Metric {
	return s.storage
}
