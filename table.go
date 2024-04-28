package pdb

import (
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
	nam string   // Table name.
	isi bool     // The table has already been initialized.
	sch *schema  // Schema.
	col []column // Table columns.
	buf []any    // Rows for inset buffer.
}

func (t *table) Select(where *where, order *order) {

	// Columns.
	col := make([]column, 0)

	// Pointers.
	poi := make([]any, 0)

	// Columns names.
	nam := make([]string, 0)

	for i := 0; i < len(t.col); i++ {
		if t.col[i].subscribed() {
			col = append(col, t.col[i])
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

	t.create()

	row, err := t.sch.dat.trx.Query(fmt.Sprintf("SELECT %s FROM %s.%s%s%s;", strings.Join(nam, ", "), t.sch.nam, t.nam, whe, ord))
	xlog.Fatalln(err)
	defer row.Close()

	for row.Next() {
		xlog.Fatalln(row.Scan(poi...))

		for i := 0; i < len(col); i++ {
			col[i].hook()
		}
	}
}

// Creates a schema and table in the database if they have not been created previously.
func (t *table) create() {

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
func (t *table) associate(col column) {

	if t.isi {
		xlog.Fatallf("Can't add column to an already initialized table: %s.", t.nam)
	} else if t.isAssociated(col) {
		xlog.Fatallf("A column named \"%s\" already exists in the table: %s.", col.name(), t.nam)
	}

	t.col = append(t.col, col)
}

// Returns this column is exist in the table.
func (t *table) isAssociated(col column) bool {

	ist := false

	for i := 0; !ist && i < len(t.col); i++ {
		ist = col == t.col[i]
	}
	return ist
}

// Returns the index of a column in a table.
func (t *table) index(col column) int {

	for i := 0; i < len(t.col); i++ {

		if col == t.col[i] {
			return i
		}

	}

	xlog.Fatallf("Column \"%s\" is not found.", col.name())

	return 0
}

func (t *table) insert(col column, val any) {

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

		_, err := t.sch.dat.trx.Exec(fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", t.sch.nam, t.nam, strings.Join(nam, ", "), strings.Join(str, ", "))+";", t.buf...)
		xlog.Fatalln(err)

		// Clear buffer.
		for i := 0; i < len(t.col); i++ {
			t.buf[i] = nil
		}
	}
}
