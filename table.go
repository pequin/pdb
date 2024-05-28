package pdb

import (
	"fmt"
	"strconv"
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

type Table struct {
	nam string   // Table name.
	isi bool     // The table has already been initialized.
	sch *Schema  // Schema.
	col []column // Table columns.
	buf []any    // Rows for inset buffer.
}

func (t *Table) Select(hook func(), where *where, order *order, offset, limit uint64, columns ...column) {

	// Pointers.
	poi := make([]any, 0)

	// Columns names.
	nam := make([]string, 0)

	if l := len(columns); l > 0 {

		for i := 0; i < l; i++ {
			poi = append(poi, columns[i].pointer())
			nam = append(nam, columns[i].name())
		}

	} else {

		for i := 0; i < len(t.col); i++ {
			poi = append(poi, t.col[i].pointer())
			nam = append(nam, t.col[i].name())
		}
	}

	// Where.
	whe := ""
	if where != nil {
		whe = " " + where.sql()
	}

	// Order.
	ord := ""
	if order != nil {
		ord = " " + order.sql()
	}

	// Limit.
	lim := ""
	if limit > 0 {
		lim = fmt.Sprintf(" LIMIT %s", strconv.FormatUint(limit, 10))
	}

	t.create()

	sss := fmt.Sprintf("SELECT %s FROM %s.%s%s%s%s OFFSET %s;", strings.Join(nam, ", "), t.sch.nam, t.nam, whe, ord, lim, strconv.FormatUint(offset, 10))
	row, err := t.sch.dat.trx.Query(sss)
	xlog.Fatallf("Error: %s, sql: %s", err, sss)
	defer row.Close()

	for row.Next() {
		xlog.Fatalln(row.Scan(poi...))

		if hook != nil {
			hook()
		}
	}
}

// Creates a schema and table in the database if they have not been created previously.
func (t *Table) create() {

	if !t.isi {
		col := make([]string, len(t.col)) // Columns.

		pri := make([]string, 0) // Primary columns.
		prk := ""                // Primary keys.
		for i := 0; i < len(t.col); i++ {
			col[i] = t.col[i].name() + " " + t.col[i].sql() + " NOT NULL"
			if t.col[i].primary() {
				pri = append(pri, t.col[i].name())
			}
		}

		if len(pri) > 0 {
			prk = fmt.Sprintf(", PRIMARY KEY(%s)", strings.Join(pri, ", "))
		}

		t.sch.create()

		_, err := t.sch.dat.trx.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s%s);", t.sch.nam, t.nam, strings.Join(col, ", "), prk))
		xlog.Fatalln(err)

		t.isi = true
	}
}

// Adds a column.
func (t *Table) associate(col column) {

	if t.isi {
		xlog.Fatallf("Can't add column to an already initialized table: %s.", t.nam)
	} else if t.isAssociated(col) {
		xlog.Fatallf("A column named \"%s\" already exists in the table: %s.", col.name(), t.nam)
	}

	t.col = append(t.col, col)
}

// Returns this column is exist in the table.
func (t *Table) isAssociated(col column) bool {

	ist := false

	for i := 0; !ist && i < len(t.col); i++ {
		ist = col == t.col[i]
	}
	return ist
}

// Returns the index of a column in a table.
func (t *Table) index(col column) int {

	for i := 0; i < len(t.col); i++ {

		if col == t.col[i] {
			return i
		}

	}

	xlog.Fatallf("Column \"%s\" is not found.", col.name())

	return 0
}

func (t *Table) insert(col column, val any) {

	// Column index.
	idx := t.index(col)

	// Initialize buffer.
	if len(t.buf) == 0 {
		t.buf = make([]any, len(t.col))
	}

	// String of values.
	str := make([]string, len(t.col))

	// Column names.
	nam := make([]string, len(t.col))
	for i := 0; i < len(t.col); i++ {
		nam[i] = t.col[i].name()
		str[i] = fmt.Sprintf("$%d", i+1)
	}

	// Save value to buffer.
	t.buf[idx] = val

	// Count of filled columns.
	cfc := 0
	for i := 0; i < len(t.col); i++ {
		if t.buf[i] != nil {
			cfc++
		}
	}

	if cfc == len(t.col) {

		t.create()

		sss := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", t.sch.nam, t.nam, strings.Join(nam, ", "), strings.Join(str, ", ")) + ";"

		// fmt.Println(t.nam, sss, t.buf)

		_, err := t.sch.dat.trx.Exec(sss, t.buf...)
		xlog.Fatallf("Error: %s, sql: \"%s\", values: %v", err, sss, val)

		// Clear buffer.
		for i := 0; i < len(t.col); i++ {
			t.buf[i] = nil
		}
	}
}
