package pdb

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
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

type Database database
type database struct {
	name    string
	server  *Server
	db      *sql.DB
	Schemas schemas
}

func (d *Database) init(name string, server *Server) error {

	name = strings.TrimSpace(name)

	if len(name) < 1 {
		return errors.New("name is not specified")
	}

	if server == nil {
		return errors.New("pointer to server is null")
	}

	d.name = name
	d.server = server

	if err := d.Schemas.init(d); err != nil {
		return err
	}

	return nil
}

func (d *Database) open() error {

	if db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", d.server.adr, strconv.FormatUint(d.server.prt, 10), d.server.usr, d.server.pwd, d.name)); err != nil {
		return err
	} else {
		d.db = db
	}

	if err := d.db.Ping(); err != nil {
		return err
	}

	return nil
}

func (d *Database) create() error {

	tdb, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=postgres sslmode=disable", d.server.adr, strconv.FormatUint(d.server.prt, 10), d.server.usr, d.server.pwd))

	if err != nil {
		return err
	}

	if err := tdb.Ping(); err != nil {
		return err
	}

	if _, err := tdb.Exec(fmt.Sprintf("CREATE DATABASE %s;", d.name)); err != nil {
		return err
	}

	return nil
}
