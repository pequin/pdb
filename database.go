package pdb

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/pequin/xlog"
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

type Database struct {
	con *sql.DB   // Database connection.
	trx *sql.Tx   // Transaction.
	isb bool      // Is bnegin started a transaction.
	sch []*schema // Scheme.
}

// Connect to the PostgreSQL database.
func Connect(user, password, host, name string) *Database {

	// Database Connection.
	con, err := sql.Open("postgres", fmt.Sprintf("host=%s port=5432 user=%s password=%s dbname=%s sslmode=disable", host, user, password, name))
	xlog.Fatalln(err)

	xlog.Fatalln(con.Ping())

	dat := Database{con: con, isb: false}
	dat.begin()

	return &dat
}

func (d *Database) Commit() {

	err := d.trx.Commit()
	d.isb = false
	if err != nil {
		xlog.Fatalln(d.trx.Rollback())
		xlog.Fatalln(err)
	}
	d.begin()
}

func (d *Database) Schema(name string) *schema {
	sch := schema{nam: name, dat: d}
	d.sch = append(d.sch, &sch)
	return &sch
}

func (d *Database) begin() {

	if !d.isb {
		tra, err := d.con.Begin()
		xlog.Fatalln(err)
		d.trx = tra
		d.isb = true
	}
}
