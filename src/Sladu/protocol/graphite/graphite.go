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
package graphite

import (
	"log"
	"net"
	"strconv"
	"time"

	"Sladu/util/stop"
)

type Config struct {
	Enable bool
	Port   int
	Bind   net.IP
}

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
		}
		log.Println(conn.RemoteAddr(), "connected")
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn *net.TCPConn) {
	defer conn.Close()
	for {
		select {
		case <-s.stopper.ShouldStop():
			log.Println("disconnecting", conn.RemoteAddr())
			return
		default:
		}
		conn.SetDeadline(time.Now().Add(1e9))
		buf := make([]byte, 4096)
		if _, err := conn.Read(buf); nil != err {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			log.Println(err)
			return
		}
		if _, err := conn.Write(buf); nil != err {
			log.Println(err)
			return
		}
	}
}
