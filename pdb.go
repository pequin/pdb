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

type database struct {
	nam string  // Database name.
	con *sql.DB // Database connection.
}

// Connect to the PostgreSQL database.
func Connect(user, password, host, name string) *database {

	// Database Connection.
	con, err := sql.Open("postgres", fmt.Sprintf("host=%s port=5432 user=%s password=%s dbname=%s sslmode=disable", host, user, password, name))
	xlog.Fatalln(err)

	xlog.Fatalln(con.Ping())

	return &database{nam: name, con: con}
}

func (d *database) Schema(name string) *Schema {
	return &Schema{nam: name, isi: false, dbc: d}
}

// type where struct {
// 	col string // Colimn name.
// 	ope string // Operator.
// 	val string // Value.
// 	// sql string // SQL type.
// }

// Returns a pointer to object where with operator "=" as equal.
// func (t *text) Where(value string) *where {
// 	return &where{col: t.nam, ope: "=", val: value}
// }

// Returns a pointer to object where with operator "=" as equal.
// func (c col) Equal() *where {
// 	return &where{col: c.nam, ope: "="}
// }

// // Returns a pointer to object where with operator ">" 	greater than.
// func (t *text) Greater(value string) *where {
// 	return &where{col: t.nam, ope: ">", val: value}
// }

// // Returns a pointer to object where with operator "<" less than.
// func (t *text) Less(value string) *where {
// 	return &where{col: t.nam, ope: "<", val: value}
// }

// // Returns a pointer to object where with operator ">=" greater than or equal.
// func (t *text) GreaterOrEqual(value string) *where {
// 	return &where{col: t.nam, ope: ">=", val: value}
// }

// // Returns a pointer to object where with operator "<=" less than or equal.
// func (t *text) LessOrEqual(value string) *where {
// 	return &where{col: t.nam, ope: "<=", val: value}
// }

// // Returns a pointer to object where with operator "<> or !=" not equal.
// func (t *text) NotEqual(value string) *where {
// 	return &where{col: t.nam, ope: "<> or !=", val: value}
// }

// func (w *where) Ggggg() {
// 	// WHERE city = 'San Francisco' AND date = '2003-07-03';

// 	// sql := w.col +"="+ w.val
// 	sql := fmt.Sprintf("%s %s '%v'", w.col, w.ope, w.val)

// 	fmt.Println(sql)
// }
