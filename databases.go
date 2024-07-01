package pdb

import (
	"errors"
	"log"
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

type databases struct {
	ser *Server // Server.
}

func (d *databases) init(server *Server) error {

	if server == nil {
		return errors.New("pointer to server is null")
	}

	d.ser = server

	return nil
}

func (d *databases) Open(name string) *Database {

	database := &Database{}

	if err := database.init(name, d.ser); err != nil {
		log.Fatalf("Error databases new: %s.", err.Error())
	}

	if err := database.open(); err != nil {
		log.Fatalf("Error databases new: %s.", err.Error())
	}

	return database

}

func (d *databases) Create(name string) *Database {

	database := &Database{}

	if err := database.init(name, d.ser); err != nil {
		log.Fatalf("Error databases create: %s.", err.Error())
	}

	if err := database.create(); err != nil {
		log.Fatalf("Error databases create: %s.", err.Error())
	}

	if err := database.open(); err != nil {
		log.Fatalf("Error databases create: %s.", err.Error())
	}

	return database

}
