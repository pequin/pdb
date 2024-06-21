package pdb

import (
	"fmt"
	"strings"
	"time"
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

type Index hash

type hash struct {
	nam string  // Name.
	hdr *header // Header
}

type into struct {
	hdr *header // Header.
	val any     // Value.
}

type header struct {
	nam string // Name.
	pgt string // Postgresql type.
	pry bool   // Is as primary.
	str *structure
}

// Returns postgresql type for this column.
type column struct {
	*header
	// *where
}

type structure struct {
	tbl *table    // Table.
	hdr []*header // Headers.
	hsh []*Index  // Indexes.
}

func (t *structure) primary() (string, bool) {

	pry := make([]string, 0)

	for i := 0; i < len(t.hdr); i++ {
		if t.hdr[i].pry {
			pry = append(pry, t.hdr[i].nam)
		}
	}

	if len(pry) > 0 {
		return fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pry, ", ")), true
	}

	return "", false
}

func (t *structure) indexes() (string, bool) {

	if len(t.hsh) < 1 {
		return "", false
	}

	hsh := make([]string, len(t.hsh))

	for i := 0; i < len(t.hsh); i++ {
		hsh[i] = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING HASH (%s);", t.hsh[i].nam, t.tbl.name(), t.hsh[i].hdr.nam)
	}

	return strings.Join(hsh, " "), true
}

// The first argument is a name of new column, and the value returned is a pointer to a column type Bool newly associated with this table.
func (t *structure) Bool(name string) *Bool {
	tpe := &Bool{header: &header{nam: name, pgt: "BOOLEAN", str: t}}
	t.hdr = append(t.hdr, tpe.header)
	return tpe
}

// The first argument is a name of new column, and the value returned is a pointer to a column type String newly associated with this table.
func (t *structure) String(name string) *String {
	tpe := &String{header: &header{nam: name, pgt: "TEXT", str: t}}
	t.hdr = append(t.hdr, tpe.header)
	return tpe
}

// The first argument is a name of new column, and the value returned is a pointer to a column type Int64 newly associated with this table.
func (t *structure) Int64(name string) *Int64 {
	tpe := &Int64{header: &header{nam: name, pgt: "BIGINT", str: t}}
	t.hdr = append(t.hdr, tpe.header)
	return tpe
}

// The first argument is a name of new column, and the value returned is a pointer to a column type Float64 newly associated with this table.
func (t *structure) Float64(name string) *Float64 {
	tpe := &Float64{header: &header{nam: name, pgt: "NUMERIC", str: t}}
	t.hdr = append(t.hdr, tpe.header)
	return tpe
}

// The first argument is a name of new column, and the value returned is a pointer to a column type Time newly associated with this table.
func (t *structure) Time(name string) *Time {
	tpe := &Time{header: &header{nam: name, pgt: "TIMESTAMP WITHOUT TIME ZONE", str: t}}
	t.hdr = append(t.hdr, tpe.header)
	return tpe
}

// Set this column is as primary.
func (h *header) AsPrimary() {
	h.pry = true
}
func (h *header) Index(name string) *Index {
	hsh := &Index{nam: name, hdr: h}
	h.str.hsh = append(h.str.hsh, hsh)
	return hsh
}

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

type Bool column

// type bollean header
type whereBool struct {
	*where
}

// Returns value from buffer.
// func (b Bool) Row(reader *reader) bool {

// 	idx, ise := reader.tbl.idx[b]
// 	if !ise {
// 		xlog.Fatallf("The column \"%s\" is not associated with the reader.", b.nam())
// 	}
// 	return *reader.buf[idx].(*bool)
// }

// func (b *Bool) Into(value bool) into {
// 	return into{col: b, val: value}
// }

// Makes new object whereString and returns pointer to it.
// func (b *Bool) Where() *whereBool {
// 	return &whereBool{where: &where{col: b}}
// }

// Returns name for this column.
// func (b Bool) name() string {
// 	return b.nam
// }

// Creates and returms pointer to buffer.
func (Bool) buf() any {
	return new(bool)
}

// Returns postgresql type for this column.
// func (tt *Bool) tpe() string {
// 	return "BOOLEAN"
// }

// Seteds operator "=" for custom value and returns pointer to object where.
func (w *whereBool) Equal(value bool) *where {
	w.where.opr = "="
	w.where.val = fmt.Sprintf("%t", value)
	return w.where
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (w *whereBool) NotEqual(value bool) *where {
	w.where.opr = "<>"
	w.where.val = fmt.Sprintf("%t", value)
	return w.where
}

// Updates custom value.
func (w *whereBool) Value(v bool) {
	w.val = fmt.Sprintf("%t", v)
}

/*
Type "String", corresponds to the type number in postgresql "TEXT",
this type implements the interface "column".
*/
// type String text
// type text header

type String column

type whereString struct {
	*where
}

// Returns value from buffer.
// func (s String) Row(reader *reader) string {

// 	idx, ise := reader.tbl.idx[s]
// 	if !ise {
// 		xlog.Fatallf("The column \"%s\" is not associated with the reader.", s.nam())
// 	}
// 	return *reader.buf[idx].(*string)
// }

// func (s *String) Into(value string) into {
// 	return into{col: s, val: value}
// }

// Makes new object whereString and returns pointer to it.
// func (s *String) Where() *whereString {
// 	return &whereString{where: &where{col: s}}
// }

// // Returns name for this column.
// func (s String) name() string {
// 	return s.nam
// }

// Creates and returms pointer to buffer.
func (String) buf() any {
	return new(string)
}

// Returns postgresql type for this column.
// func (String) tpe() string {
// 	return "TEXT"
// }

// Seteds operator "<=" for custom value and returns pointer to object where.
func (w *whereString) LessOrEqual(value string) *where {
	w.where.opr = "<="
	w.where.val = value
	return w.where
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (w *whereString) Equal(value string) *where {
	w.where.opr = "="
	w.where.val = value
	return w.where
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (w *whereString) NotEqual(value string) *where {
	w.where.opr = "<>"
	w.where.val = value
	return w.where
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (w *whereString) Greater(value string) *where {
	w.where.opr = ">"
	w.where.val = value
	return w.where
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (w *whereString) GreaterOrEqual(value string) *where {
	w.where.opr = ">="
	w.where.val = value
	return w.where
}

// Updates custom value.
func (w *whereString) Value(v string) {
	w.val = v
}

/*
Type "Int64", corresponds to the type number in postgresql "BIGINT",
this type implements the interface "column".
*/
// type Int64 bigint
// type bigint header

type Int64 column

type whereInt64 struct {
	*where
}

// Returns value from buffer.
// func (i Int64) Row(reader *reader) int64 {

// 	idx, ise := reader.tbl.idx[i]
// 	if !ise {
// 		xlog.Fatallf("The column \"%s\" is not associated with the reader.", i.nam())
// 	}
// 	return *reader.buf[idx].(*int64)
// }

func (i *Int64) Into(value int64) into {
	return into{hdr: i.header, val: value}
}

// Makes new object whereByInt64 and returns pointer to it.
// func (i *Int64) Where() *whereInt64 {
// 	return &whereInt64{where: &where{col: i}}
// }

// Returns name for this column.
// func (i Int64) name() string {
// 	return i.nam
// }

// Creates and returms pointer to buffer.
func (Int64) buf() any {
	return new(int64)
}

// Returns postgresql type for this column.
func (Int64) tpe() string {
	return "BIGINT"
}

// Seteds operator "<=" for custom value and returns pointer to object where.
func (w *whereInt64) LessOrEqual(value int64) *where {
	w.where.opr = "<="
	w.where.val = fmt.Sprintf("%d", value)
	return w.where
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (w *whereInt64) Equal(value int64) *where {
	w.where.opr = "="
	w.where.val = fmt.Sprintf("%d", value)
	return w.where
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (w *whereInt64) NotEqual(value int64) *where {
	w.where.opr = "<>"
	w.where.val = fmt.Sprintf("%d", value)
	return w.where
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (w *whereInt64) Greater(value int64) *where {
	w.where.opr = ">"
	w.where.val = fmt.Sprintf("%d", value)
	return w.where
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (w *whereInt64) GreaterOrEqual(value int64) *where {
	w.where.opr = ">="
	w.where.val = fmt.Sprintf("%d", value)
	return w.where
}

// Updates custom value.
func (w *whereInt64) Value(v int64) {
	w.val = fmt.Sprintf("%d", v)
}

/*
Type "Float64", corresponds to the type number in postgresql "NUMERIC",
this type implements the interface "column".
*/
// type Float64 numeric
// type numeric header

type Float64 column
type whereFloat64 struct {
	*where
}

// Returns value from buffer.
// func (f Float64) Row(reader *reader) float64 {

// 	idx, ise := reader.tbl.idx[f]
// 	if !ise {
// 		xlog.Fatallf("The column \"%s\" is not associated with the reader.", f.nam())
// 	}
// 	return *reader.buf[idx].(*float64)
// }

// func (f *Float64) Into(value float64) into {
// 	return into{col: f, val: value}
// }

// Makes new object whereFloat64 and returns pointer to it.
// func (f *Float64) Where() *whereFloat64 {
// 	return &whereFloat64{where: &where{col: f}}
// }

// // Returns name for this column.
// func (f Float64) nam() string {
// 	return strings.ToLower(string(f))
// }

// Create and returms pointer to buffer.
func (Float64) buf() any {
	return new(float64)
}

// Returns postgresql type for this column.
// func (Float64) tpe() string {
// 	return "NUMERIC"
// }

// Seteds operator "<=" for custom value and returns pointer to object where.
func (w *whereFloat64) LessOrEqual(value float64) *where {
	w.where.opr = "<="
	w.where.val = fmt.Sprintf("%f", value)
	return w.where
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (w *whereFloat64) Equal(value float64) *where {
	w.where.opr = "="
	w.where.val = fmt.Sprintf("%f", value)
	return w.where
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (w *whereFloat64) NotEqual(value float64) *where {
	w.where.opr = "<>"
	w.where.val = fmt.Sprintf("%f", value)
	return w.where
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (w *whereFloat64) Greater(value float64) *where {
	w.where.opr = ">"
	w.where.val = fmt.Sprintf("%f", value)
	return w.where
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (w *whereFloat64) GreaterOrEqual(value float64) *where {
	w.where.opr = ">="
	w.where.val = fmt.Sprintf("%f", value)
	return w.where
}

// Updates custom value.
func (w *whereFloat64) Value(v float64) {
	w.val = fmt.Sprintf("%f", v)
}

/*
Type "Time", corresponds to the type number in postgresql "TIMESTAMP WITHOUT TIME ZONE",
this type implements the interface "column".
*/
// type Time timestamp
// type timestamp header

type Time column

type whereTime struct {
	*where
}

// Returns value from buffer.
// func (t Time) Row(reader *reader) time.Time {

// 	idx, ise := reader.tbl.idx[t]
// 	if !ise {
// 		xlog.Fatallf("The column \"%s\" is not associated with the reader.", t.nam())
// 	}
// 	return *reader.buf[idx].(*time.Time)
// }

// func (t *Time) Into(value time.Time) into {
// 	value = value.UTC()
// 	return into{col: t, val: value}
// }

// Makes new object whereTime and returns pointer to it.
// func (t *Time) Where() *whereTime {
// 	return &whereTime{where: &where{col: t}}
// }

// // Returns name for this column.
// func (t Time) nam() string {
// 	return strings.ToLower(string(t))
// }

// Create and returms pointer to buffer.
func (Time) buf() any {
	return new(time.Time)
}

// Returns postgresql type for this column.
// func (Time) tpe() string {
// 	return "TIMESTAMP WITHOUT TIME ZONE"
// }

// Seteds operator "<=" for custom value and returns pointer to object where.
func (w *whereTime) LessOrEqual(value time.Time) *where {
	value = value.UTC()
	w.where.opr = "<="
	w.where.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.where
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (w *whereTime) Equal(value time.Time) *where {
	value = value.UTC()
	w.where.opr = "="
	w.where.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.where
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (w *whereTime) NotEqual(value time.Time) *where {
	value = value.UTC()
	w.where.opr = "<>"
	w.where.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.where
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (w *whereTime) Greater(value time.Time) *where {
	value = value.UTC()
	w.where.opr = ">"
	w.where.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.where
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (w *whereTime) GreaterOrEqual(value time.Time) *where {
	value = value.UTC()
	w.where.opr = ">="
	w.where.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.where
}

// Updates custom value.
func (w *whereTime) Value(v time.Time) {
	v = v.UTC()
	w.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second(), v.Nanosecond())
}
