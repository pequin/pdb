package pdb

import (
	"fmt"
	"strings"
	"time"

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

// type column interface {
// 	nam() string // Returns name of the column.
// 	buf() any    // Creates pointer to buffer and and returns pointer to it.
// 	tpe() string // Returns postgresql type for this column.
// }

// type Index hash

type index struct {
	nme string // Name of column.
	opr string // Operator.
	val string // Value.
}

// type header struct {
// 	nam string // Name.
// 	pgt string // Postgresql type.
// 	pry bool   // Is as primary.
// 	str *types
// }

// Returns postgresql type for this column.
// type column struct {
// 	*header
// 	// *where
// }

type Type interface {
	name() string  // Name for this column.
	primary() bool //Is primary.
	indexed() bool // Is indexed.
	sql() string   // Postgresql type.

}

type types struct {
	tbl *table  // Table.
	ser *serial // Serial column.
	cls []Type  // Columns.

	// tbl *table // Table.
	// cls []Type // Columns.
	// nme []string     // Names of columns.
	// idx map[Type]int // Indexes of columns.
	// ci map[Type]string // Headers.

	// hsh []*Index  // Indexes.
}

// func (t *types) primary() (string, bool) {

// 	pry := make([]string, 0)

// 	for i := 0; i < len(t.hdr); i++ {
// 		if t.hdr[i].pry {
// 			pry = append(pry, t.hdr[i].nam)
// 		}
// 	}

// 	if len(pry) > 0 {
// 		return fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pry, ", ")), true
// 	}

// 	return "", false
// }

// func (t *types) indexes() (string, bool) {

// 	if len(t.hsh) < 1 {
// 		return "", false
// 	}

// 	hsh := make([]string, len(t.hsh))

// 	for i := 0; i < len(t.hsh); i++ {
// 		hsh[i] = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING HASH (%s);", t.hsh[i].nam, t.tbl.name(), t.hsh[i].hdr.nam)
// 	}

// 	return strings.Join(hsh, " "), true
// }

type column struct {
	tbl *table
	nme string // Name.
	pry bool   // Is primary.
	idx bool   // Is indexed.
}

// func (c *column) write(v any) {
// }

// Returns name for this column.
func (c *column) name() string {
	return c.nme
}

// Is primary.
func (c *column) primary() bool {
	return c.pry
}

// Is indexed.
func (c *column) indexed() bool {
	return c.idx
}

// type hash struct {
// 	// nme string // Name of column.

// 	// 	col column // Column.
// 	// 	opr string // Operator.
// 	// 	val string // Value.

// 	// nam string // Name.
// 	// hdr *header // Header
// }

// func (index) Index() {
// }

// The first argument is a name of new column, and the value returned is a pointer to a column type boolean newly associated with this table.
func (t *types) Bool(name string) *boolean {
	typ := &boolean{column{tbl: t.tbl, nme: name, pry: false, idx: false}}
	t.cls = append(t.cls, typ)
	return typ
}

// The first argument is a name of new column, and the value returned is a pointer to a column type text newly associated with this table.
func (t *types) String(name string) *text {
	typ := &text{column{tbl: t.tbl, nme: name, pry: false, idx: false}}
	t.cls = append(t.cls, typ)
	return typ
}

// The first argument is a name of new column, and the value returned is a pointer to a column type bigint newly associated with this table.
func (t *types) Int64(name string) *bigint {
	typ := &bigint{column{tbl: t.tbl, nme: name, pry: false, idx: false}}
	t.cls = append(t.cls, typ)
	return typ
}

// The first argument is a name of new column, and the value returned is a pointer to a column type serial newly associated with this table.
func (t *types) Serial(name string) *serial {
	if t.ser == nil {
		t.ser = &serial{column{tbl: t.tbl, nme: name, pry: true, idx: false}}
	} else {
		xlog.Fatallf("A column of type series \"%s\" is already associated with the table: %s", t.ser.nme, t.tbl.nme)
	}
	return t.ser
}

// The first argument is a name of new column, and the value returned is a pointer to a column type numeric newly associated with this table.
func (t *types) Float64(name string) *numeric {
	typ := &numeric{column{tbl: t.tbl, nme: name, pry: false, idx: false}}
	t.cls = append(t.cls, typ)
	return typ
}

// The first argument is a name of new column, and the value returned is a pointer to a column type timestamp newly associated with this table.
func (t *types) Time(name string) *timestamp {
	typ := &timestamp{column{tbl: t.tbl, nme: name, pry: false, idx: false}}
	t.cls = append(t.cls, typ)
	return typ
}

// Create indexes.
func (t *types) createIndexes() {

	qry := make([]string, 0)

	for i := 0; i < len(t.cls); i++ {

		if t.cls[i].indexed() {
			qry = append(qry, fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING HASH (%s);", t.cls[i].name(), t.tbl.name(), t.cls[i].name()))
		}
	}

	_, err := t.tbl.stx.Exec(strings.Join(qry, " "))
	xlog.Fatalln(err)
}

func (t *types) primary() []string {
	p := make([]string, 0)

	for i := 0; i < len(t.cls); i++ {

		if t.cls[i].primary() {
			p = append(p, t.cls[i].name())
		}

	}

	return p
}

// hsh[i] = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING HASH (%s);", t.hsh[i].nam, t.tbl.name(), t.hsh[i].hdr.nam)

// // Headers
// // formats according to a format specifier and returns the resulting string.
// func (t *types) headers(format func(name, sql string) string) {

// }

// Set this column is as primary.
// func (h *header) AsPrimary() {
// 	h.pry = true
// }
// func (h *header) Index(name string) *Index {
// 	hsh := &Index{nam: name, hdr: h}
// 	h.str.hsh = append(h.str.hsh, hsh)
// 	return hsh
// }

// Returns name for this column.
// func (c column) name() string {
// 	return c.nam
// }

// func (h header) nam() string {
// 	return strings.ToLower(h.namm)
// }

// func (header) buf() any {
// 	return new(bool)
// }

// // Returns postgresql type for this column.
// func (header) tpe() string {
// 	return "ffff"
// }

/*
Type "Bool", corresponds to the type number in postgresql "BOOLEAN",
this type implements the interface "column".
*/
type boolean struct {
	column
}

type indexBoolean struct {
	idx *index
}

// Write value to a row buffer.
func (b *boolean) Write(value bool) {
	b.tbl.write(b, value)
}

// Makes new object indexBoolean and returns pointer to it.
func (b *boolean) NewIndex() *indexBoolean {
	b.idx = true
	return &indexBoolean{idx: &index{nme: b.nme}}
}

// Returns postgresql type.
func (boolean) sql() string {
	return "BOOLEAN"
}

// Creates and returms pointer to buffer.
// func (Bool) buf() any {
// 	return new(bool)
// }

// Seteds operator "=" for custom value and returns pointer to object indexBoolean.
func (i *indexBoolean) Equal(value bool) *index {
	i.idx.opr = "="
	i.idx.val = fmt.Sprintf("%t", value)
	return i.idx
}

// Seteds operator "<>" for custom value and returns pointer to object indexBoolean.
func (i *indexBoolean) NotEqual(value bool) *index {
	i.idx.opr = "<>"
	i.idx.val = fmt.Sprintf("%t", value)
	return i.idx
}

// // Updates custom value.
// func (w *whereBool) Value(v bool) {
// 	w.val = fmt.Sprintf("%t", v)
// }

/*
Type "String", corresponds to the type number in postgresql "TEXT",
this type implements the interface "column".
*/
// type String text
// type text header

type text struct {
	column
}

type indexText struct {
	idx *index
}

// Write value to a row buffer.
func (t *text) Write(value string) {
	t.tbl.write(t, value)
}

// Set as primary.
func (t *text) AsPrimary() *text {
	t.pry = true
	return t
}

// Makes new object indexText and returns pointer to it.
func (t *text) NewIndex() *indexText {
	t.idx = true
	return &indexText{idx: &index{nme: t.nme}}
}

// Returns name for this column.
// func (t *text) name() string {
// 	return t.nme
// }

// Returns postgresql type.
func (text) sql() string {
	return "TEXT"
}

// This type is serial.
func (text) serial() bool {
	return false
}

// Creates and returms pointer to buffer.
// func (String) buf() any {
// 	return new(string)
// }

// i.opr = "="
// i.val = fmt.Sprintf("%t", value)
// return i

// Seteds operator "<=" for custom value and returns pointer to object where.
func (i *indexText) LessOrEqual(value string) *index {
	i.idx.opr = "<="
	i.idx.opr = value
	return i.idx
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (i *indexText) Equal(value string) *index {
	i.idx.opr = "="
	i.idx.opr = value
	return i.idx
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (i *indexText) NotEqual(value string) *index {
	i.idx.opr = "<>"
	i.idx.opr = value
	return i.idx
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (i *indexText) Greater(value string) *index {
	i.idx.opr = ">"
	i.idx.opr = value
	return i.idx
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (i *indexText) GreaterOrEqual(value string) *index {
	i.idx.opr = ">="
	i.idx.opr = value
	return i.idx
}

/*
Type "Int64", corresponds to the type number in postgresql "BIGINT",
this type implements the interface "column".
*/
// type Int64 bigint
// type bigint header

type bigint struct {
	column
}

type indexBigint struct {
	idx *index
}

// Write value to a row buffer.
func (b *bigint) Write(value int64) {
	b.tbl.write(b, value)
}
func (b *bigint) Update(value int64, filer string) {
	// b.tbl.write(b, value)
}

// Set as primary.
func (b *bigint) AsPrimary() *bigint {
	b.pry = true
	return b
}

// Makes new object indexBigint and returns pointer to it.
func (b *bigint) NewIndex() *indexBigint {
	b.idx = true
	return &indexBigint{idx: &index{nme: b.nme}}
}

// primary

// Returns name for this column.
// func (b *bigint) name() string {
// 	return b.nme
// }

// Returns postgresql type.
func (bigint) sql() string {
	return "BIGINT"
}

// This type is serial.
func (bigint) serial() bool {
	return false
}

/*
Type "Int64", corresponds to the type number in postgresql "BIGINT",
this type implements the interface "column".
*/
// type Int64 bigint
// type bigint header

type serial struct {
	column
}

type indexSerial struct {
	idx *index
}

// primary

// Returns name for this column.
// func (b *bigint) name() string {
// 	return b.nme
// }

// Returns postgresql type.
func (serial) sql() string {
	return "BIGSERIAL"
}

// Creates and returms pointer to buffer.
// func (Int64) buf() any {
// 	return new(int64)
// }

// Seteds operator "<=" for custom value and returns pointer to object where.
func (i *indexSerial) LessOrEqual(value int64) *index {
	i.idx.opr = "<="
	i.idx.opr = fmt.Sprintf("%d", value)
	return i.idx
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (i *indexSerial) Equal(value int64) *index {
	i.idx.opr = "="
	i.idx.opr = fmt.Sprintf("%d", value)
	return i.idx
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (i *indexSerial) NotEqual(value int64) *index {
	i.idx.opr = "<>"
	i.idx.opr = fmt.Sprintf("%d", value)
	return i.idx
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (i *indexSerial) Greater(value int64) *index {
	i.idx.opr = ">"
	i.idx.opr = fmt.Sprintf("%d", value)
	return i.idx
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (i *indexSerial) GreaterOrEqual(value int64) *index {
	i.idx.opr = ">="
	i.idx.opr = fmt.Sprintf("%d", value)
	return i.idx
}

/*
Type "Float64", corresponds to the type number in postgresql "NUMERIC",
this type implements the interface "column".
*/
// type Float64 numeric
// type numeric header

type numeric struct {
	column
}

type indexNumeric struct {
	idx *index
}

// Write value to a row buffer.
func (n *numeric) Write(value float64) {
	n.tbl.write(n, value)
}

// Set as primary.
func (n *numeric) AsPrimary() *numeric {
	n.pry = true
	return n
}

// Makes new object indexNumeric and returns pointer to it.
func (n *numeric) NewIndex() *indexNumeric {
	n.idx = true
	return &indexNumeric{idx: &index{nme: n.nme}}
}

// Returns name for this column.
// func (n *numeric) name() string {
// 	return n.nme
// }

// Returns postgresql type.
func (numeric) sql() string {
	return "NUMERIC"
}

// type whereFloat64 struct {
// 	*where
// }

// Returns value from buffer.
// func (f Float64) Row(reader *reader) float64 {

// 	idx, ise := reader.tbl.idx[f]
// 	if !ise {
// 		xlog.Fatallf("The column \"%s\" is not associated with the reader.", f.nam())
// 	}
// 	return *reader.buf[idx].(*float64)
// }

// Seteds operator "<=" for custom value and returns pointer to object where.
func (i *indexNumeric) LessOrEqual(value float64) *index {
	i.idx.opr = "<="
	i.idx.opr = fmt.Sprintf("%f", value)
	return i.idx
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (i *indexNumeric) Equal(value float64) *index {
	i.idx.opr = "="
	i.idx.opr = fmt.Sprintf("%f", value)
	return i.idx
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (i *indexNumeric) NotEqual(value float64) *index {
	i.idx.opr = "<>"
	i.idx.opr = fmt.Sprintf("%f", value)
	return i.idx
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (i *indexNumeric) Greater(value float64) *index {
	i.idx.opr = ">"
	i.idx.opr = fmt.Sprintf("%f", value)
	return i.idx
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (i *indexNumeric) GreaterOrEqual(value float64) *index {
	i.idx.opr = ">="
	i.idx.opr = fmt.Sprintf("%f", value)
	return i.idx
}

/*
Type "Time", corresponds to the type number in postgresql "TIMESTAMP WITHOUT TIME ZONE",
this type implements the interface "column".
*/
// type Time timestamp
// type timestamp header

type timestamp struct {
	column
}

type indexTimestamp struct {
	idx *index
}

// Write value to a row buffer.
func (t *timestamp) Write(value time.Time) {
	t.tbl.write(t, value)
}

// Set as primary.
func (t *timestamp) AsPrimary() *timestamp {
	t.pry = true
	return t
}

// Makes new object indexTimestamp and returns pointer to it.
func (t *timestamp) NewIndex() *indexTimestamp {
	t.idx = true
	return &indexTimestamp{idx: &index{nme: t.nme}}
}

// Returns name for this column.
// func (t *timestamp) name() string {
// 	return t.nme
// }

// Returns postgresql type.
func (timestamp) sql() string {
	return "TIMESTAMP WITHOUT TIME ZONE"
}

// Create and returms pointer to buffer.
// func (Time) buf() any {
// 	return new(time.Time)
// }

// Seteds operator "<=" for custom value and returns pointer to object where.
func (i *indexTimestamp) LessOrEqual(value time.Time) *index {
	value = value.UTC()
	i.idx.opr = "<="
	i.idx.opr = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return i.idx
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (i *indexTimestamp) Equal(value time.Time) *index {
	value = value.UTC()
	i.idx.opr = "="
	i.idx.opr = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return i.idx
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (i *indexTimestamp) NotEqual(value time.Time) *index {
	value = value.UTC()
	i.idx.opr = "<>"
	i.idx.opr = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return i.idx
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (i *indexTimestamp) Greater(value time.Time) *index {
	value = value.UTC()
	i.idx.opr = ">"
	i.idx.opr = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return i.idx
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (i *indexTimestamp) GreaterOrEqual(value time.Time) *index {
	value = value.UTC()
	i.idx.opr = ">="
	i.idx.opr = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return i.idx
}
