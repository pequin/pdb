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
	nme string            // Name.
	sdb *schema           // Schema of database.
	col []column          // Columns.
	idx map[column]int    // Column indexes.
	cpk []column          // Columns of primary keys.
	uqe []column          // Unique columns.
	hsh map[column]string // Hash columns.
	stx *sql.Tx           // Transaction.
}

func (s *schema) Table(name string, columns ...column) *table {
	if len(columns) < 1 {
		xlog.Fatalln("When creating new table, you must specify at least one column.")
	}

	t := &table{nme: strings.ToLower(name), sdb: s, col: columns, idx: make(map[column]int), hsh: make(map[column]string)}

	for i := 0; i < len(columns); i++ {
		t.idx[columns[i]] = i
	}

	t.begin()

	return t
}

// Begin starts a transaction and rolls back the previous transaction.
func (t *table) begin() {

	if t.stx != nil {
		t.stx.Rollback()
	}

	stx, err := t.sdb.con.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
	xlog.Fatalln(err)
	t.stx = stx
}

// Commit commits the transaction.
func (t *table) commit() {

	if t.stx != nil {
		t.stx.Commit()
	}

	t.begin()
}

// Returns the string which includes the name of the scheme and table.
func (t *table) name() string {
	return fmt.Sprintf("%s.%s", t.sdb.nme, t.nme)
}

func (t *table) Primary(col column) {

	idx := t.indexOfColumn(col)

	if idx >= 0 {
		t.cpk = append(t.cpk, col)
	} else {
		xlog.Fatallf("This column \"%s\" is not associated with this structure", col.nam())
	}
}

func (t *table) Unique(col column) {

	idx := t.indexOfColumn(col)

	if idx >= 0 {
		t.uqe = append(t.uqe, col)
	} else {
		xlog.Fatallf("This column \"%s\" is not associated with this structure", col.nam())
	}
}
func (t *table) Hash(name string, col column) {

	idx := t.indexOfColumn(col)

	if idx >= 0 {
		t.hsh[col] = name
	} else {
		xlog.Fatallf("This column \"%s\" is not associated with this structure", col.nam())
	}
}

func (t *table) indexOfColumn(col column) int {

	for i := 0; i < len(t.col); i++ {
		if col == t.col[i] {
			return i
		}
	}

	return -1
}

func (t *table) primaryColimns() string {

	if len(t.cpk) < 1 {
		return ""
	}

	cpk := make([]string, len(t.cpk))

	for i := 0; i < len(t.cpk); i++ {
		cpk[i] = t.col[i].nam()
	}

	return fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(cpk, ", "))
}
func (t *table) uniqueColumns() string {

	if len(t.uqe) < 1 {
		return ""
	}

	idc := make([]string, len(t.uqe))

	for i := 0; i < len(t.uqe); i++ {
		idc[i] = t.uqe[i].nam()
	}

	return fmt.Sprintf("UNIQUE (%s)", strings.Join(idc, ", "))
}
func (t *table) hashColumns() string {

	if len(t.hsh) < 1 {
		return ""
	}

	hsh := make([]string, 0)

	for col, nam := range t.hsh {
		hsh = append(hsh, fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING HASH (%s);", nam, t.name(), col.nam()))

	}

	return strings.Join(hsh, " ")
}

func (t *table) Create() {

	col := make([]string, len(t.col))

	for i := 0; i < len(t.col); i++ {
		col[i] = fmt.Sprintf("%s %s NOT NULL", t.col[i].nam(), t.col[i].tpe())
	}

	pmy := t.primaryColimns()
	uqe := t.uniqueColumns()

	if pmy != "" {
		pmy = ", " + pmy
	}
	if uqe != "" {
		uqe = ", " + uqe
	}

	_, err := t.stx.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;CREATE TABLE IF NOT EXISTS %s (%s%s%s);", t.sdb.nme, t.name(), strings.Join(col, ", "), pmy, uqe))
	xlog.Fatalln(err)

	hsh := t.hashColumns()

	if hsh != "" {
		_, err := t.stx.Exec(hsh)
		xlog.Fatalln(err)
	}

	t.commit()
}

type writer struct {
	tbl *table // Table.
	// col []column // Columns.
	// col map[column][]any // Columns.

	// buf []any

}

// INSERT INTO

func (t *table) Writer() *writer {
	return &writer{tbl: t}
}

func (w *writer) Fff(inset ...into) {

	// if _, err := c.database.Exec(sql, setter.values...); err != nil {

}
