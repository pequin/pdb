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
	nme string  // Name.
	sdb *schema // Schema of database.
	stx *sql.Tx // Transaction.
}

func (s *schema) Table(name string) *table {
	return &table{nme: strings.ToLower(name), sdb: s}
}

// Begin starts a transaction and rolls back the previous transaction.
func (t *table) Begin() {

	if t.stx != nil {
		t.stx.Rollback()
	}

	stx, err := t.sdb.con.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
	xlog.Fatalln(err)
	t.stx = stx
}

// Returns the string which includes the name of the scheme and table.
func (t *table) name() string {
	return fmt.Sprintf("%s.%s", t.sdb.nme, t.nme)
}

// ////////////////////////////////
type structure struct {
	tbl *table   // Table.
	col []column // Columns.
	cpk []column // Columns of primary keys.
	// ind map[column]int // Column indexes.
	// col map[column]int // Columns.
}

func (t *table) Structure(columns ...column) *structure {

	if len(columns) < 1 {
		xlog.Fatalln("When creating new table structure, you must specify at least one column.")
	}

	s := &structure{tbl: t, col: columns, cpk: make([]column, 0)}

	return s
}

func (s *structure) Primary(col column) {

	idx := s.indexOfColumn(col)

	if idx >= 0 {
		s.cpk = append(s.cpk, col)
	} else {
		xlog.Fatallf("This column \"%s\" is not associated with this structure", col.nam())
	}
}

func (s *structure) indexOfColumn(col column) int {

	for i := 0; i < len(s.col); i++ {
		if col == s.col[i] {
			return i
		}
	}

	return -1
}

func (s *structure) primary() string {

	if len(s.cpk) < 1 {
		return ""
	}

	cpk := make([]string, len(s.cpk))

	for i := 0; i < len(s.cpk); i++ {
		cpk[i] = s.col[i].nam()
	}

	return fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(cpk, ", "))
}

func (s *structure) Create() {

	col := make([]string, len(s.col))

	for i := 0; i < len(s.col); i++ {
		col[i] = fmt.Sprintf("%s %s NOT NULL", s.col[i].nam(), s.col[i].tpe())
	}

	pmy := s.primary()

	if pmy != "" {
		pmy = ", " + pmy
	}

	str := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;CREATE TABLE IF NOT EXISTS %s (%s%s)", s.tbl.sdb.nme, s.tbl.name(), strings.Join(col, ", "), pmy)

	_, err := s.tbl.sdb.con.ExecContext(context.Background(), str)
	xlog.Fatalln(err)
}
