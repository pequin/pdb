package pdb

import (
	"context"
	"database/sql"
	"fmt"

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

//	type structure struct {
//		tbl *table   // Table.
//		col []column // Columns.
//		// ind map[column]int // Column indexes.
//		// col map[column]int // Columns.
//	}
type table struct {
	nme string  // Name.
	sdb *schema // Schema of database.
	stx *sql.Tx // Transaction.
}

func (s *schema) Table(name string) *table {
	return &table{nme: name, sdb: s}
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
func (t *table) from() string {
	return fmt.Sprintf("%s.%s", t.sdb.nme, t.nme)
}

// func (t *table) Structure() *structure {
// 	return &structure{tbl: t, col: make([]column, 0)}
// }

// func (s *structure) indexOfColumn(col column) int {

// 	for i := 0; i < len(s.col); i++ {
// 		if col == s.col[i] {
// 			return i
// 		}
// 	}

// 	return -1
// }

// func (s *structure) columnByIndex(ind int) column {
// 	return nil
// }

// // primary key
// func (s *structure) Column(col column, primary bool) {

// 	idx := s.indexOfColumn(col)

// 	fmt.Println("dbf", idx)

// 	s.col = append(s.col, col)

// 	// heap.
// 	// 	fmt.Println("gfn", c)
// 	// s.col[c] = 1
// }

// Returns the names of the columns.
// func (t *table) columns() string {

// 	col := make([]string, len(t.col))

// 	for i := 0; i < len(t.col); i++ {
// 		col[i] = t.from() + "." + t.col[i].nam()
// 	}

// 	return strings.Join(col, ", ")
// }

//////////////////////////////////////////////////////////////////////

// type reader struct {
// 	rtx *rtx            // Context for read transaction.

// }

// func (t *table) Reader() *reader {
// 	return &reader{}
// }

// 	// sss := "aaasssss"

// 	// columns[0].ptr()

// }

// 	// Pointers.
// 	ptr := make([]any, len(columns))

// 	for i := 0; i < len(columns); i++ {
// 	}

// 	// for i := 0; i < len(ptr); i++ {
// 	// 	// ptr[i]
// 	// }

// 	row()

// 	// // 	// Columns names.
// 	// // 	nam := make([]string, 0)

// 	// // 	if l := len(columns); l > 0 {

// 	// // 		for i := 0; i < l; i++ {
// 	// // 			poi = append(poi, columns[i].pointer())
// 	// // 			nam = append(nam, columns[i].name())
// 	// // 		}

// 	// // 	} else {

// 	fmt.Println("dfb", ptr)
// }

// func (t *table) Bool(name string) *boolean {
// 	v := boolean{nm: name}
// 	t.cl = append(t.cl, &v)
// 	return &v
// }
// func (t *table) String(name string) *text {
// 	v := text{nm: name}
// 	t.cl = append(t.cl, &v)
// 	return &v
// }
// func (t *table) Int64(name string) *bigint {
// 	v := bigint{nm: name}
// 	t.cl = append(t.cl, &v)
// 	return &v
// }
// func (t *table) Float64(name string) *numeric {
// 	v := numeric{nm: name}
// 	t.cl = append(t.cl, &v)
// 	return &v
// }
// func (t *table) Time(name string) *timestamp {
// 	v := timestamp{nm: name}
// 	t.cl = append(t.cl, &v)
// 	return &v
// }

// func (t *table) where() {

// 	s := ""

// 	for i := 0; i < len(t.cl); i++ {
// 		s += t.cl[i].whr() + " "
// 	}

// 	s = strings.TrimSpace(s)
// 	s = strings.TrimSuffix(s, "AND")
// 	s = strings.TrimSuffix(s, "OR")
// 	s = strings.TrimSpace(s)

// 	fmt.Println("ssssssssssss", s)
// }

// func (t *table) Select(offset, limit uint64, row func(), columns ...column) {

// 	sl := "*" // Select.

// 	if len(columns) > 0 {
// 		sl = t.namesOfSpecifiedColumns(columns...)
// 	} else {
// 		sl = t.namesOfAllColumns()
// 	}

// 	t.where()

// 	// sss := fmt.Sprintf("SELECT %s FROM %s.%s%s%s%s OFFSET %s;", strings.Join(nam, ", "), t.sch.nam, t.nam, whe, ord, lim, strconv.FormatUint(offset, 10))
// 	// sss := fmt.Sprintf("SELECT %s FROM %s;", sl, t.from())

// 	// 	row, err := t.sch.dat.trx.Query(sss)
// 	// 	xlog.Fatallf("Error: %s, sql: %s", err, sss)
// 	// 	defer row.Close()

// 	// 	for row.Next() {
// 	// 		xlog.Fatalln(row.Scan(poi...))

// 	// 		if hook != nil {
// 	// 			hook()
// 	// 		}
// 	// 	}
// 	// }

// 	fmt.Println("db", sl)

// 	// 	// Columns names.
// 	// 	nam := make([]string, 0)

// 	// 	if l := len(columns); l > 0 {

// 	// 		for i := 0; i < l; i++ {
// 	// 			poi = append(poi, columns[i].pointer())
// 	// 			nam = append(nam, columns[i].name())
// 	// 		}

// 	// 	} else {

// 	// 		for i := 0; i < len(t.col); i++ {
// 	// 			poi = append(poi, t.col[i].pointer())
// 	// 			nam = append(nam, t.col[i].name())
// 	// 		}
// 	// 	}

// 	// fmt.Println("bdf", tables)

// }

// // type get struct {
// // 	fr string // From.
// // 	sl string // Select.
// // 	wr *where // Where.
// // 	// sb []*order // Sort by.
// // 	// SELECT * FROM kraken.s_1inch_zeur WHERE s_1inch_zeur.trade <=5
// // }

// // price
// // SELECT testproduct_id, product_name, category_name
// // FROM testproducts
// // INNER JOIN categories ON testproducts.category_id = categories.category_id;

// // func (t *table) Get(filter *where) *get {
// // 	return &get{fr: fmt.Sprintf("%s.%s", t.sh.nm, t.nm), sl: t.columns(), wr: filter}
// // }

// // Returns the names of the columns.
// func (t *table) namesOfAllColumns() string {

// 	nc := make([]string, len(t.cl))

// 	for i := 0; i < len(t.cl); i++ {
// 		nc[i] = t.from() + "." + t.cl[i].nme()
// 	}

// 	return strings.Join(nc, ", ")
// }

// // Returns the names of the specified columns.
// func (t *table) namesOfSpecifiedColumns(columns ...column) string {

// 	nc := make([]string, 0)

// 	for id := 0; id < len(columns); id++ {

// 		for i := 0; i < len(t.cl); i++ {

// 			if columns[id] == t.cl[i] {
// 				nc = append(nc, t.from()+"."+t.cl[i].nme())
// 			}

// 		}
// 	}

// 	return strings.Join(nc, ", ")
// }

// // func (g get) Equal() {

// // }

// // func (t *table) indexOfColumn(c column) int {

// // 	for i := 0; i < len(t.cl); i++ {

// // 		if t.cl[i] == c {
// // 			return i
// // 		}
// // 	}
// // 	return -1
// // }

// // func (t *table) nameOfColumn(c column) string {
// // 	return t.nc[t.indexOfColumn(c)]
// // }
// // func (t *table) typeOfColumn(c column) string {
// // 	return t.tc[t.indexOfColumn(c)]
// // }

// // func (t *table) from() string {
// // 	return fmt.Sprintf("%s.%s", t.sh.nm, t.nm)
// // }

// // 	sss := fmt.Sprintf("SELECT %s FROM %s.%s%s%s%s OFFSET %s;", strings.Join(nam, ", "), t.sch.nam, t.nam, whe, ord, lim, strconv.FormatUint(offset, 10))
// // 	row, err := t.sch.dat.trx.Query(sss)
// // 	xlog.Fatallf("Error: %s, sql: %s", err, sss)
// // 	defer row.Close()

// // 	for row.Next() {
// // 		xlog.Fatalln(row.Scan(poi...))

// // 		if hook != nil {
// // 			hook()
// // 		}
// // 	}
// // }

// // type where struct {
// // }

// // func (t *Table) Select(hook func(), where *where, order *order, offset, limit uint64, columns ...column) {

// // 	// Pointers.
// // 	poi := make([]any, 0)

// // 	// Columns names.
// // 	nam := make([]string, 0)

// // 	if l := len(columns); l > 0 {

// // 		for i := 0; i < l; i++ {
// // 			poi = append(poi, columns[i].pointer())
// // 			nam = append(nam, columns[i].name())
// // 		}

// // 	} else {

// // 		for i := 0; i < len(t.col); i++ {
// // 			poi = append(poi, t.col[i].pointer())
// // 			nam = append(nam, t.col[i].name())
// // 		}
// // 	}

// // 	// Where.
// // 	whe := ""
// // 	if where != nil {
// // 		whe = " " + where.sql()
// // 	}

// // 	// Order.
// // 	ord := ""
// // 	if order != nil {
// // 		ord = " " + order.sql()
// // 	}

// // 	// Limit.
// // 	lim := ""
// // 	if limit > 0 {
// // 		lim = fmt.Sprintf(" LIMIT %s", strconv.FormatUint(limit, 10))
// // 	}

// // 	t.create()

// // 	sss := fmt.Sprintf("SELECT %s FROM %s.%s%s%s%s OFFSET %s;", strings.Join(nam, ", "), t.sch.nam, t.nam, whe, ord, lim, strconv.FormatUint(offset, 10))
// // 	row, err := t.sch.dat.trx.Query(sss)
// // 	xlog.Fatallf("Error: %s, sql: %s", err, sss)
// // 	defer row.Close()

// // 	for row.Next() {
// // 		xlog.Fatalln(row.Scan(poi...))

// // 		if hook != nil {
// // 			hook()
// // 		}
// // 	}
// // }

// // CREATE TABLE IF NOT
// // func (s *schema) Table(name string, columns ...column) *table {

// // 	cn := make([]string, len(columns)) // Column names.
// // 	ct := make([]string, len(columns)) // Column types.
// // 	pc := make([]string, 0)            // Primary columns.

// // 	for i := 0; i < len(columns); i++ {
// // 		cn[i] = columns[i].name()
// // 		ct[i] = columns[i].sql()
// // 		if columns[i].primary() {
// // 			pc = append(pc)
// // 		}
// // 	}

// // 	// 		if len(pri) > 0 {
// // 	// 			prk = fmt.Sprintf(", PRIMARY KEY(%s)", strings.Join(pri, ", "))
// // 	// 		}

// // 	// 		t.sch.create()

// // 	fmt.Println("dfb", cn, " - ", ct)

// // 	// _, err := t.sch.dat.trx.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s%s);", t.sch.nam, t.nam, strings.Join(col, ", "), prk))
// // 	// 		xlog.Fatalln(err)

// // 	return &table{nm: name}
// // }

// // func (t *table) New() *new_table {
// // 	return &new_table{tb: t}
// // }

// // func (t *table) Create(columns ...column) *new_table {

// // }

// // func (n *new_table) Add(column column, name string, primary bool) {

// // 	for i := 0; i < len(n.cn); i++ {

// // 	}

// // }

// // columns ...column

// // func (t *table) Create(column func(), columns ...column) {

// // }

// // // Creates a schema and table in the database if they have not been created previously.
// // func (t *Table) create() {

// // 	if !t.isi {
// // 		col := make([]string, len(t.col)) // Columns.

// // 		pri := make([]string, 0) // Primary columns.
// // 		prk := ""                // Primary keys.
// // 		for i := 0; i < len(t.col); i++ {
// // 			col[i] = t.col[i].name() + " " + t.col[i].sql() + " NOT NULL"
// // 			if t.col[i].primary() {
// // 				pri = append(pri, t.col[i].name())
// // 			}
// // 		}

// // 		if len(pri) > 0 {
// // 			prk = fmt.Sprintf(", PRIMARY KEY(%s)", strings.Join(pri, ", "))
// // 		}

// // 		t.sch.create()

// // 		_, err := t.sch.dat.trx.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s%s);", t.sch.nam, t.nam, strings.Join(col, ", "), prk))
// // 		xlog.Fatalln(err)

// // 		t.isi = true
// // 	}
// // }

// // // Adds a column.
// // func (t *Table) associate(col column) {

// // 	if t.isi {
// // 		xlog.Fatallf("Can't add column to an already initialized table: %s.", t.nam)
// // 	} else if t.isAssociated(col) {
// // 		xlog.Fatallf("A column named \"%s\" already exists in the table: %s.", col.name(), t.nam)
// // 	}

// // 	t.col = append(t.col, col)
// // }

// // // Returns this column is exist in the table.
// // func (t *Table) isAssociated(col column) bool {

// // 	ist := false

// // 	for i := 0; !ist && i < len(t.col); i++ {
// // 		ist = col == t.col[i]
// // 	}
// // 	return ist
// // }

// // // Returns the index of a column in a table.
// // func (t *Table) index(col column) int {

// // 	for i := 0; i < len(t.col); i++ {

// // 		if col == t.col[i] {
// // 			return i
// // 		}

// // 	}

// // 	xlog.Fatallf("Column \"%s\" is not found.", col.name())

// // 	return 0
// // }

// // func (t *Table) insert(col column, val any) {

// // 	// Column index.
// // 	idx := t.index(col)

// // 	// Initialize buffer.
// // 	if len(t.buf) == 0 {
// // 		t.buf = make([]any, len(t.col))
// // 	}

// // 	// String of values.
// // 	str := make([]string, len(t.col))

// // 	// Column names.
// // 	nam := make([]string, len(t.col))
// // 	for i := 0; i < len(t.col); i++ {
// // 		nam[i] = t.col[i].name()
// // 		str[i] = fmt.Sprintf("$%d", i+1)
// // 	}

// // 	// Save value to buffer.
// // 	t.buf[idx] = val

// // 	// Count of filled columns.
// // 	cfc := 0
// // 	for i := 0; i < len(t.col); i++ {
// // 		if t.buf[i] != nil {
// // 			cfc++
// // 		}
// // 	}

// // 	if cfc == len(t.col) {

// // 		t.create()

// // 		sss := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", t.sch.nam, t.nam, strings.Join(nam, ", "), strings.Join(str, ", ")) + ";"

// // 		// fmt.Println(t.nam, sss, t.buf)

// // 		_, err := t.sch.dat.trx.Exec(sss, t.buf...)
// // 		xlog.Fatallf("Error: %s, sql: \"%s\", values: %v", err, sss, val)

// // 		// Clear buffer.
// // 		for i := 0; i < len(t.col); i++ {
// // 			t.buf[i] = nil
// // 		}
// // 	}
// // }
