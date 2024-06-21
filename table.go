package pdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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

type table struct {
	nme    string  // Name.
	sdb    *schema // Schema of database.
	stx    *sql.Tx // Transaction
	Column structure
	isi    bool // This table is init
	// idx map[column]int    // Column indexes.
	// cpk []column          // Columns of primary keys.
	// hsh []*Index // Hash columns.

}

func (s *schema) Table(name string) *table {
	t := &table{nme: strings.ToLower(name), sdb: s, isi: false}
	t.Column.tbl = t
	t.begin()
	return t
}

// Commit commits the transaction.
func (t *table) Commit() {

	if t.stx != nil {

		t.init()
		t.stx.Commit()
	}

	t.begin()
}

// Returns the string which includes the name of the scheme and table name.
func (t *table) name() string {
	return fmt.Sprintf("%s.%s", t.sdb.nme, t.nme)
}

func (t *table) init() {

	if t.isi {
		hap := make([]string, len(t.Column.hdr)) // Headers and primary keys.

		for i := 0; i < len(t.Column.hdr); i++ {
			hap[i] = fmt.Sprintf("%s %s NOT NULL", t.Column.hdr[i].nam, t.Column.hdr[i].pgt)
		}

		if s, b := t.Column.primary(); b {
			hap = append(hap, s)
		}

		qry := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s; CREATE TABLE IF NOT EXISTS %s (%s);", t.sdb.nme, t.name(), strings.Join(hap, ", "))
		_, err := t.stx.Exec(qry)
		xlog.Fatalln(err)

		if q, b := t.Column.indexes(); b {
			_, err := t.stx.Exec(q)
			xlog.Fatalln(err)
		}
	}

}

// Begin starts a transaction and rolls back the previous transaction.
func (t *table) begin() {
	//
	// if t.stx != nil {
	// 	t.stx.Rollback()
	// }

	stx, err := t.sdb.con.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
	xlog.Fatalln(err)
	t.stx = stx
}
