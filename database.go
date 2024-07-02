package pdb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
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
	nam     string  // Name.
	ser     *Server // Server.
	pgd     *sql.DB // PostgreSQL database.
	ttn     *sql.Tx // Transaction.
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

	d.nam = name
	d.ser = server

	if err := d.Schemas.init(d); err != nil {
		return err
	}

	return nil
}

func (d *Database) open() error {

	if pgd, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", d.ser.ads, strconv.FormatUint(d.ser.prt, 10), d.ser.uer, d.ser.pwd, d.nam)); err != nil {
		return err
	} else {
		d.pgd = pgd
	}

	if err := d.pgd.Ping(); err != nil {
		return err
	}

	return nil
}

func (d *Database) create() error {

	pgd, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=postgres sslmode=disable", d.ser.ads, strconv.FormatUint(d.ser.prt, 10), d.ser.uer, d.ser.pwd))

	if err != nil {
		return err
	}

	if err := pgd.Ping(); err != nil {
		return err
	}

	if _, err := pgd.Exec(fmt.Sprintf("CREATE DATABASE %s;", d.nam)); err != nil {
		return err
	}

	return nil
}

// func (d *Database) begin() error {

// 	if d.ttn == nil {

// 		ttn, err := d.pgd.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
// 		if err != nil {
// 			return err
// 		}

// 		d.ttn = ttn
// 	}

// 	return nil
// }

func (d *Database) commit() error {
	if d.ttn == nil {
		return fmt.Errorf("pointer to transaction is null")
	}

	if err := d.ttn.Commit(); err != nil {

		if err := d.ttn.Rollback(); err != nil {
			return err
		}

		return err
	}

	d.ttn = nil

	return nil

}

func (d *Database) Commit() {

	if err := d.commit(); err != nil {
		log.Fatalf("Error database commit: %s.", err.Error())
	}
}
