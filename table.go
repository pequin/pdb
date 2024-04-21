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
	sch *Schema  // Table Schema.
	col []column // Table columns.
	pri []column // Primary columns.
	isi bool     // The base is initialized.
}

// Creates a schema and table in the database if they have not been created previously.
func (t *table) create() {

	sql := make([]string, 0)

	if !t.sch.isi {
		sql = append(sql, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", t.sch.nam))
	}

	if !t.isi {

		// Columns.
		col := make([]string, len(t.col))

		for i := 0; i < len(t.col); i++ {
			col[i] = t.col[i].Name() + " " + t.col[i].Type() + " NOT NULL"
		}

		// Primary key.
		// pri := make([]string, len(t.pri))
		prk := t.columnPrimaryNames()
		pri := ""

		if len(t.pri) > 0 {
			pri = fmt.Sprintf(", PRIMARY KEY(%s)", strings.Join(prk, ", "))
		}

		sql = append(sql, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s%s);", t.sch.nam, t.nam, strings.Join(col, ", "), pri))
	}

	_, err := t.sch.dbc.con.Exec(strings.Join(sql, " "))
	xlog.Fatalln(err)
}

// Set column as base.
func (t *table) addColumn(col column) {
	if t.isi {
		xlog.Fatallf("Can't add column to an already initialized table: %s.", t.nam)
	} else if t.isColumnExist(col) {
		xlog.Fatallf("The column \"%s\" has already been added as a base column to the table: %s.", col.Name(), t.nam)
	} else if t.isColumnNameExist(col.Name()) {
		xlog.Fatallf("A column named \"%s\" already exists in the table: %s.", col.Name(), t.nam)
	} else {
		t.col = append(t.col, col)
	}
}

// Returns this column is exist in the table.
func (t *table) isColumnExist(col column) bool {

	ist := false

	for i := 0; !ist && i < len(t.col); i++ {
		ist = col == t.col[i]
	}
	return ist
}

// Returns column names.
func (t *table) columnNames() []string {

	col := make([]string, len(t.col))

	for i := 0; i < len(t.col); i++ {
		col[i] = t.col[i].Name()
	}
	return col
}

// Returns the names of the primary columns.
func (t *table) columnPrimaryNames() []string {

	col := make([]string, len(t.pri))

	for i := 0; i < len(t.pri); i++ {
		col[i] = t.pri[i].Name()
	}
	return col
}

// Returns true if a column with this name exists.
func (t *table) isColumnNameExist(name string) bool {

	ist := false

	for i := 0; !ist && i < len(t.col); i++ {
		ist = name == t.col[i].Name()
	}

	return ist
}

// Returns true if the column is primary.
func (t *table) isColumnPrimary(col column) bool {

	ist := false

	for i := 0; !ist && i < len(t.pri); i++ {
		ist = col == t.pri[i]
	}
	return ist
}

// Sets primary columns.
func (t *table) Primary(columns ...column) {

	if t.isi {
		xlog.Fatallf("Can't add primary columns to an already initialized table: %s.", t.nam)
	} else {

		for i := 0; i < len(columns); i++ {

			if !t.isColumnExist(columns[i]) {
				xlog.Fatallf("Column \"%s\" is not associated with table: %s.", columns[i].Name(), t.nam)
			}

			if t.isColumnPrimary(columns[i]) {
				xlog.Fatallf("Column \"%s\" already added as primary to the table: %s.", columns[i].Name(), t.nam)
			}

			t.pri = append(t.pri, columns[i])
		}
	}
}

// Creates an buffer [row][column].
func (t *table) buffer() [][]any {

	row := 0

	for i := 0; i < len(t.col); i++ {

		len := t.col[i].size()

		if row < len {
			row = len
		}
	}

	buf := make([][]any, row)

	for i := 0; i < row; i++ {
		buf[i] = make([]any, len(t.col))
	}

	// Parse colimns.
	for cid := 0; cid < len(t.col); cid++ {

		b := t.col[cid].buffer()

		// Parse rows.
		for rid := 0; rid < len(b); rid++ {
			buf[rid][cid] = b[rid]
		}
	}

	return buf
}

func (t *table) Insert() {

	// Previously added data.
	buf := t.buffer()

	// Values for insert.
	val := make([]any, 0, len(t.col)*len(buf))

	// String of values.
	stv := make([]string, len(buf))

	// Value id.
	vid := 1
	for rid := 0; rid < len(buf); rid++ {

		// Row data.
		row := make([]string, len(t.col))

		for cid := 0; cid < len(t.col); cid++ {

			if buf[rid][cid] == nil {
				xlog.Fatallf("Error adding data to table \"%s\", in row \"%d\" is missing value for column \"%s\".", t.nam, rid, t.col[cid].Name())
			}

			row[cid] = fmt.Sprintf("$%d", vid)

			val = append(val, buf[rid][cid])

			vid++
		}

		stv[rid] = "(" + strings.Join(row, ", ") + ")"
	}

	if len(val) == 0 {
		xlog.Fatallf("Missing values in buffer to insert table: %s.", t.nam)
	}

	t.create()

	_, err := t.sch.dbc.con.Exec(fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES %s", t.sch.nam, t.nam, strings.Join(t.columnNames(), ", "), strings.Join(stv, ", "))+";", val...)
	xlog.Fatalln(err)
}

type getter struct {
	tab *table
	col []column
}

// Returns a pointer to an object getter.
func (t *table) Select(columns ...column) *getter {

	t.create()

	// Columns.
	col := make([]column, 0)

	if len(columns) == 0 {
		col = append(col, t.col...)
	} else {
		for i := 0; i < len(columns); i++ {
			if !t.isColumnExist(columns[i]) {
				xlog.Fatallf("When calling select, a column \"%s\" is not associated with the table: %s.", columns[i].Name(), t.nam)
			}
		}
		col = append(col, columns...)
	}

	return &getter{tab: t, col: col}
}

func (g *getter) Run() {

	// Pointers.
	poi := make([]any, len(g.col))

	// Columns names.
	col := make([]string, len(g.col))

	for i := 0; i < len(g.col); i++ {
		poi[i] = g.col[i].pointer()
		col[i] = g.col[i].Name()
	}

	sql := fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY pair ASC;", strings.Join(col, ", "), g.tab.sch.nam, g.tab.nam)
	fmt.Println("Select:", sql)

	row, err := g.tab.sch.dbc.con.Query(sql)
	xlog.Fatalln(err)
	defer row.Close()

	for row.Next() {
		xlog.Fatalln(row.Scan(poi...))

		for i := 0; i < len(g.col); i++ {
			g.col[i].hook()
		}
	}

	xlog.Fatalln(row.Err())

}

// func (t *Table) Init() {

// if t.isi {

// }

// if !t.isInit {
// 	sql := "CREATE SCHEMA IF NOT EXISTS %[1]s; CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (%[3]s%[4]s)"

// 	// Colums.
// 	cls := make([]string, len(t.columns))

// 	// Primary colums.
// 	pcs := make([]string, 0)

// 	// Primary.
// 	pmy := ""

// 	for i := 0; i < len(t.columns); i++ {
// 		cls[i] = fmt.Sprintf("%s %s NOT NULL", t.columns[i].Name(), t.columns[i].Type())

// 		if t.columns[i].Primary() {
// 			pcs = append(pcs, t.columns[i].Name())
// 		}
// 	}

// 	// Primary.
// 	if len(pcs) > 0 {
// 		pmy = fmt.Sprintf(", PRIMARY KEY(%s)", strings.Join(pcs, ", "))
// 	}

// 	sql = fmt.Sprintf(sql, t.schema.name, t.name, strings.Join(cls, ", "), pmy)

// 	_, err := t.schema.connection.database.Exec(sql)
// 	xlog.Fatalln(err)

// 	t.isInit = true

// }
// }
