package pdb

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

/*
Copyright 2024 Vasiliy Vdovin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

type Options struct {
	Address  string
	Port     uint64
	Username string
	Password string
}

type Server server
type server struct {
	Databases databases
	adr       string
	prt       uint64
	usr       string
	pwd       string
}

func (s *Server) init(opt *Options) error {

	adr := strings.TrimSpace(opt.Address)
	usr := strings.TrimSpace(opt.Username)
	pwd := strings.TrimSpace(opt.Password)

	if len(adr) < 1 {
		return errors.New("server init: address is not specified")
	}
	if len(usr) < 1 {
		return errors.New("server init: username is not specified")
	}
	if len(pwd) < 1 {
		return errors.New("server init: password is not specified")
	}

	s.adr = adr
	s.prt = opt.Port
	s.usr = usr
	s.pwd = pwd

	if err := s.Databases.init(s); err != nil {
		return fmt.Errorf("server init: %w", err)
	}

	return nil
}

func NewServer(options *Options) *Server {

	s := &Server{}

	if err := s.init(options); err != nil {
		log.Fatalf("NewServer: %s.", err.Error())
	}

	return s
}
