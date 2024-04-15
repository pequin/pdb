package pdb

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/pequin/xlog"
)

// Copyright 2024 Vasiliy Vdovin

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

type Type uint64

type Connection struct {
	database *sql.DB
}

type column struct {
	name     string
	primary  bool
	datatype Type
}

type Table struct {
	name       string
	schema     string
	connection *Connection
	columns    []*column
	data       [][]any
}

const (
	Bool = iota
	Int64
	Float64
	String
	Time
)

// Connect to the PostgreSQL database.
func NewConnection(user, password, host, database string) *Connection {

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=5432 user=%s password=%s dbname=%s sslmode=disable", host, user, password, database))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatalln(err)
	}

	return &Connection{db}
}

func (t *Type) SQL() string {

	switch *t {
	case Bool:
		return "BOOL"
	case Int64:
		return "INT8"
	case Float64:
		return "NUMERIC"
	case String:
		return "TEXT"
	case Time:
		return "TIMESTAMP WITHOUT TIME ZONE"
	}

	xlog.Fatalln("Type is not found.")

	return ""
}

func typeFromSQL(tpe *sql.ColumnType) *Type {

	var t Type

	switch tpe.DatabaseTypeName() {
	case "BOOL":
		t = Bool
	case "INT8":
		t = Int64
	case "NUMERIC":
		t = Float64
	case "TEXT":
		t = String
	case "TIMESTAMP":
		t = Time
	default:
		xlog.Fatallf("Type %s is not found.", tpe.DatabaseTypeName())
	}

	return &t
}

func (t *Type) Pointer() any {

	switch *t {
	case Bool:
		var v bool
		return &v
	case Int64:
		var v int64
		return &v
	case Float64:
		var v float64
		return &v
	case String:
		var v string
		return &v
	case Time:
		var v time.Time
		return &v
	}

	xlog.Fatalln("Type is not found.")

	return nil
}

func Column(name string, datatype Type, primary bool) column {
	return column{name: name, primary: primary, datatype: datatype}
}

func (c *Connection) Table(schema, name string, columns ...*column) *Table {

	sql := "CREATE SCHEMA IF NOT EXISTS %[1]s; CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (%[3]s%[4]s)"

	// Colums.
	cls := make([]string, len(columns))

	// Primary colums.
	pcs := make([]string, 0)

	// Primary.
	pmy := ""

	for i := 0; i < len(columns); i++ {
		cls[i] = fmt.Sprintf("%s %s NOT NULL", columns[i].name, columns[i].datatype.SQL())

		if columns[i].primary {
			pcs = append(pcs, columns[i].name)
		}
	}

	// Primary.
	if len(pcs) > 0 {
		pmy = fmt.Sprintf(", PRIMARY KEY(%s)", strings.Join(pcs, ", "))
	}

	sql = fmt.Sprintf(sql, schema, name, strings.Join(cls, ", "), pmy)

	_, err := c.database.Exec(sql)
	xlog.Fatalln(err)

	tbe := Table{name: name, schema: schema, connection: c, columns: columns, data: make([][]any, 0)}

	return &tbe
}

func (t *Table) AddRow(data ...any) {

	for cID := 0; cID < len(data); cID++ {
		if reflect.TypeOf(data[cID]) == reflect.TypeOf(time.Time{}) {
			data[cID] = data[cID].(time.Time).UTC()
		}
	}

	if len(t.columns) == len(data) {
		t.data = append(t.data, data)
	} else {
		xlog.Fatalln("The number of values must equal the number of columns.")
	}
}

func (t *Table) Commit() {

	// Values.
	vls := make([]any, 0)

	// Column names.
	cns := make([]string, len(t.columns))

	// Columns.
	cms := make([]string, len(t.columns))

	// Rows.
	rws := make([]string, len(t.data))

	dID := 1
	for rID := 0; rID < len(t.data); rID++ {

		for cID := 0; cID < len(t.data[rID]); cID++ {

			cms[cID] = fmt.Sprintf("$%d", dID)

			vls = append(vls, t.data[rID][cID])

			dID++
		}

		rws[rID] = fmt.Sprintf("(%s)", strings.Join(cms, ","))
	}

	for cID := 0; cID < len(t.columns); cID++ {
		cns[cID] = t.columns[cID].name
	}

	sql := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES %s", t.schema, t.name, strings.Join(cns, ","), strings.Join(rws, ","))

	_, err := t.connection.database.Exec(sql, vls...)
	xlog.Fatalln(err)

	t.data = t.data[:0]
}

type Select struct {
	table   *Table
	columns []*column
}

func (t *Table) Select(columns ...*column) *Select {

	slt := Select{table: t}

	// Is match.
	ism := false

	for cID := 0; cID < len(columns); cID++ {

		ism = false

		for id := 0; !ism && id < len(t.columns); id++ {
			ism = columns[cID] == t.columns[id]
		}

		if ism {
			slt.columns = append(slt.columns, columns[cID])
		} else {
			xlog.Fatallf("Column \"%s\" does not match the one specified in the table.", columns[cID].name)
		}

	}

	return &slt
}

func (s *Select) Rows(row func(columns ...any)) {

	cls := make([]string, len(s.columns))

	for i := 0; i < len(s.columns); i++ {
		cls[i] = s.columns[i].name
	}

	sql := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(cls, ", "), s.table.schema, s.table.name)

	rws, err := s.table.connection.database.Query(sql)
	xlog.Fatalln(err)
	defer s.table.connection.database.Close()
	defer rws.Close()

	// Column types.
	cts, err := rws.ColumnTypes()
	xlog.Fatalln(err)

	san := make([]any, len(cts))

	for i := 0; i < len(cts); i++ {
		san[i] = typeFromSQL(cts[i]).Pointer()
	}

	for rws.Next() {
		xlog.Fatalln(rws.Scan(san...))
		row(san...)
	}
}
