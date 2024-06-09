package pdb

import (
	"fmt"
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

type column interface {
	nam() string // Returns name of column .
	// 	// 	// 	// whr() string // Returns where string.
	// 	// 	// 	// pgt() string // Returns postgresql type for this column.
	// 	// 	// 	// bff() any    // Returns a pointer to a buffer.

	buf() any // Create and returm pointer to buffer.
	// 	// ptr(func() string) string // Create and returm pointer
	// row(func(gg *Get)) // Create and returm pointer
	// buf() any // Create and returm variable for buffer
}

/*
Type "boolean", corresponds to the type number in postgresql "BOOLEAN",
this type implements the interface "column".
*/
// type boolean struct {
// 	nme string // Name of column.
// 	ptr *bool  // Pointer to buffer.
// }

// // Creates object type bool corresponding in postgresql as boolean.
// func Bool(name string) *boolean {
// 	return &boolean{nme: name}
// }

// // Returns name for this column.
// func (b *boolean) name() string {
// 	return b.nme
// }

// // Create and returm pointer to buffer.
// func (b *boolean) buffer() any {
// 	b.ptr = new(bool)
// 	return b.ptr
// }

// // Update value for filter.
// func (b *boolean) Where(value bool) {
// 	*b.wv = fmt.Sprintf("'%t'", value)
// }

// // Establishes the logic of the association where.
// func (b *boolean) And() {
// 	*b.wu = "AND"
// }

// // Establishes the logic of the association where.
// func (b *boolean) Or() {
// 	*b.wu = "OR"
// }

// // Sets the filter equal for type boolean.
// func (b *boolean) Equal(value bool) {
// 	v := fmt.Sprintf("'%t'", value)
// 	o := "="
// 	u := "AND"
// 	b.wo = &o
// 	b.wv = &v
// 	b.wu = &u
// }

// // Sets the filter not equal for type boolean.
// func (b *boolean) NotEqual(value bool) {
// 	v := fmt.Sprintf("'%t'", value)
// 	o := "<>"
// 	u := "AND"
// 	b.wo = &o
// 	b.wv = &v
// 	b.wu = &u
// }

// // Sets sorting by asc.
// func (b *boolean) Asc() {
// 	v := true
// 	b.sb = &v
// }

// // Sets sorting by desc.
// func (b *boolean) Desc() {
// 	v := false
// 	b.sb = &v
// }

// // Returns name for this column.
// func (b *boolean) nme() string {
// 	return b.nm
// }

// // Returns where string.
// func (b *boolean) whr() string {

// 	v := ""

// 	if b.wo != nil && b.wv != nil {
// 		v = b.nm + " " + *b.wo + " " + *b.wv
// 	}

// 	if b.wu != nil {
// 		v += " " + *b.wu
// 	}

// 	return v
// }

// // Returns a pointer to a buffer.
// func (b *boolean) bff() any {
// 	return &b.rw
// }

// // Returns postgresql type for this column.
// func (boolean) pgt() string {
// 	return "BOOLEAN"
// }

// /*
// Type "text", corresponds to the type number in postgresql "TEXT",
// this type implements the interface "column".
// */
// type text struct {
// 	nm string  // Name.
// 	rw string  // Row for select buffer.
// 	wo *string // Where operator.
// 	wv *string // Where value.
// 	wu *string // Where union.
// 	sb *bool   // Sort by.
// }

// // Update value for filter.
// func (t *text) Where(value string) {
// 	*t.wv = value
// }

// // Establishes the logic of the association where.
// func (t *text) And() {
// 	*t.wu = "AND"
// }

// // Establishes the logic of the association where.
// func (t *text) Or() {
// 	*t.wu = "OR"
// }

// // Sets the filter equal for type text.
// func (t *text) Equal(value string) {
// 	v := value
// 	o := "="
// 	u := "AND"
// 	t.wo = &o
// 	t.wv = &v
// 	t.wu = &u
// }

// // Sets the filter not equal for type text.
// func (t *text) NotEqual(value string) {
// 	v := value
// 	o := "<>"
// 	u := "AND"
// 	t.wo = &o
// 	t.wv = &v
// 	t.wu = &u
// }

// // Sets sorting by asc.
// func (t *text) Asc() {
// 	v := true
// 	t.sb = &v
// }

// // Sets sorting by desc.
// func (t *text) Desc() {
// 	v := false
// 	t.sb = &v
// }

// // Returns name for this column.
// func (t *text) nme() string {
// 	return t.nm
// }

// // Returns where string.
// func (t *text) whr() string {

// 	v := ""

// 	if t.wo != nil && t.wv != nil {
// 		v = t.nm + " " + *t.wo + " " + *t.wv
// 	}

// 	if t.wu != nil {
// 		v += " " + *t.wu
// 	}

// 	return v
// }

// // Returns a pointer to a buffer.
// func (t *text) bff() any {
// 	return &t.rw
// }

// // Returns postgresql type for this column.
// func (text) pgt() string {
// 	return "TEXT"
// }

/*
Type "Int64", corresponds to the type number in postgresql "BIGINT",
this type implements the interface "column".
*/
type Int64 string
type whereInt64 struct {
	whr *where
}

// Returns value from buffer.
func (i Int64) Row(reader *reader) int64 {
	return *reader.buf[reader.rid[i]].(*int64)
}

// Creates object type where.
// func (i Int64) Where(value int64) *where {
// return &where{col: i, val: fmt.Sprintf("'%d'", value), opr: "="}
// }
// func (i Int64) Where(where *where, value int64) {
// 	return &where{col: i, val: fmt.Sprintf("'%d'", value), opr: "="}
// }

// col column // Column.
// opr string // Operator.
// val string // Value.
// Update value for where.
func (i Int64) Where() *whereInt64 {
	return &whereInt64{whr: &where{col: i}}
}

func (w *whereInt64) Less(value int64) *where {
	w.whr.opr = "<"
	w.whr.val = fmt.Sprintf("%d", value)
	return w.whr
}
func (w *whereInt64) LessOrEqual(value int64) *where {
	w.whr.opr = "<="
	w.whr.val = fmt.Sprintf("%d", value)
	return w.whr
}
func (w *whereInt64) Equal(value int64) *where {
	w.whr.opr = "="
	w.whr.val = fmt.Sprintf("%d", value)
	return w.whr
}
func (w *whereInt64) NotEqual(value int64) *where {
	w.whr.opr = "<>"
	w.whr.val = fmt.Sprintf("%d", value)
	return w.whr
}
func (w *whereInt64) Greater(value int64) *where {
	w.whr.opr = ">"
	w.whr.val = fmt.Sprintf("%d", value)
	return w.whr
}
func (w *whereInt64) GreaterOrEqual(value int64) *where {
	w.whr.opr = ">="
	w.whr.val = fmt.Sprintf("%d", value)
	return w.whr
}

// Creates a sort object with a default value - asc.
// func (i Int64) Sort() *order {
// 	return &order{nme: i.nam(), asc: true}
// }

// Returns name for this column.
func (i Int64) nam() string {
	return string(i)
}

// Creates and returm pointer to buffer.
func (Int64) buf() any {
	return new(int64)
}

// // Update value for filter.
// func (n *Int64) Set(filter *where, value int64) {
// 	filter.val = fmt.Sprintf("'%d'", value)
// }

// // Establishes the logic of the association where.
// func (b *bigint) And() {
// 	*b.wu = "AND"
// }

// // Establishes the logic of the association where.
// func (b *bigint) Or() {
// 	*b.wu = "OR"
// }

// // Sets the filter less than for type bigint.
// func (b *bigint) LessThan(value int64) {
// 	v := fmt.Sprintf("'%d'", value)
// 	o := "<"
// 	u := "AND"
// 	b.wo = &o
// 	b.wv = &v
// 	b.wu = &u
// }

// // Sets the filter less or equal for type bigint.
// func (b *bigint) LessOrEqual(value int64) {
// 	v := fmt.Sprintf("'%d'", value)
// 	o := "<="
// 	u := "AND"
// 	b.wo = &o
// 	b.wv = &v
// 	b.wu = &u
// }

// // Sets the filter equal for type bigint.
// func (b *bigint) Equal(value int64) {
// 	v := fmt.Sprintf("'%d'", value)
// 	o := "="
// 	u := "AND"
// 	b.wo = &o
// 	b.wv = &v
// 	b.wu = &u
// }

// // Sets the filter not equal for type bigint.
// func (b *bigint) NotEqual(value int64) {
// 	v := fmt.Sprintf("'%d'", value)
// 	o := "<>"
// 	u := "AND"
// 	b.wo = &o
// 	b.wv = &v
// 	b.wu = &u
// }

// // Sets the filter greater than for type bigint.
// func (b *bigint) GreaterThan(value int64) {
// 	v := fmt.Sprintf("'%d'", value)
// 	o := ">"
// 	u := "AND"
// 	b.wo = &o
// 	b.wv = &v
// 	b.wu = &u
// }

// // Sets the filter greater or equal for type bigint.
// func (b *bigint) GreaterOrEqual(value int64) {
// 	v := fmt.Sprintf("'%d'", value)
// 	o := ">="
// 	u := "AND"
// 	b.wo = &o
// 	b.wv = &v
// 	b.wu = &u
// }

// // Sets sorting by asc.
// func (b *bigint) Asc() {
// 	v := true
// 	b.sb = &v
// }

// // Sets sorting by desc.
// func (b *bigint) Desc() {
// 	v := false
// 	b.sb = &v
// }

// // Returns name for this column.
// func (b *bigint) nme() string {
// 	return b.nm
// }

// // Returns where string.
// func (b *bigint) whr() string {

// 	v := ""

// 	if b.wo != nil && b.wv != nil {
// 		v = b.nm + " " + *b.wo + " " + *b.wv
// 	}

// 	if b.wu != nil {
// 		v += " " + *b.wu
// 	}

// 	return v
// }

// // Returns a pointer to a buffer.
// func (b *bigint) bff() any {
// 	return &b.rw
// }

// // Returns postgresql type for this column.
// func (bigint) pgt() string {
// 	return "BIGINT"
// }

/*
Type "numeric", corresponds to the type number in postgresql "NUMERIC",
this type implements the interface "column".
*/
type Float64 string
type whereFloat64 struct {
	whr *where
}

// type numeric struct {
// 	nme string   // Name of column.
// 	ptr *float64 // Pointer to buffer.
// }

// // Creates object type float64 corresponding in postgresql as numeric.
// func Float64(name string) *numeric {
// 	return &numeric{nme: name}
// }

// // Returns name for this column.
// func (n *numeric) name() string {
// 	return n.nme
// }

// Returns value from buffer.
func (f Float64) Row(reader *reader) float64 {
	return *reader.buf[reader.rid[f]].(*float64)
}

// // Creates object type where.
// func (f Float64) Where(value float64) *where {
// 	return &where{col: f, val: fmt.Sprintf("'%f'", value), opr: "="}
// }

// // Update value for where.
// func (f *Float64) WhereValue(where *where, value float64) {
// 	if f == where.col {
// 		where.val = fmt.Sprintf("'%f'", value)
// 	} else {
// 		xlog.Fatalln("Cannot update value for *where for another column.")
// 	}
// }

// Creates a sort object with a default value - asc.
// func (f Float64) Sort() *order {
// 	return &order{nme: f.nam(), asc: true}
// }

// Returns name for this column.
func (f Float64) nam() string {
	return string(f)
}

// Create and returm pointer to buffer.
func (Float64) buf() any {
	return new(float64)
}

// // Update value for filter.
// func (n *numeric) Where(value float64) {
// 	*n.wv = fmt.Sprintf("'%f'", value)
// }

// // Establishes the logic of the association where.
// func (n *numeric) And() {
// 	*n.wu = "AND"
// }

// // Establishes the logic of the association where.
// func (n *numeric) Or() {
// 	*n.wu = "OR"
// }

// // Sets the filter less than for type numeric.
// func (n *numeric) LessThan(value float64) {
// 	v := fmt.Sprintf("'%f'", value)
// 	o := "<"
// 	u := "AND"
// 	n.wo = &o
// 	n.wv = &v
// 	n.wu = &u
// }

// // Sets the filter less or equal for type numeric.
// func (n *numeric) LessOrEqual(value float64) {
// 	v := fmt.Sprintf("'%f'", value)
// 	o := "<="
// 	u := "AND"
// 	n.wo = &o
// 	n.wv = &v
// 	n.wu = &u
// }

// // Sets the filter equal for type numeric.
// func (n *numeric) Equal(value float64) {
// 	v := fmt.Sprintf("'%f'", value)
// 	o := "="
// 	u := "AND"
// 	n.wo = &o
// 	n.wv = &v
// 	n.wu = &u
// }

// // Sets the filter not equal for type numeric.
// func (n *numeric) NotEqual(value float64) {
// 	v := fmt.Sprintf("'%f'", value)
// 	o := "<>"
// 	u := "AND"
// 	n.wo = &o
// 	n.wv = &v
// 	n.wu = &u
// }

// // Sets the filter greater than for type numeric.
// func (n *numeric) GreaterThan(value float64) {
// 	v := fmt.Sprintf("'%f'", value)
// 	o := ">"
// 	u := "AND"
// 	n.wo = &o
// 	n.wv = &v
// 	n.wu = &u
// }

// // Sets the filter greater or equal for type numeric.
// func (n *numeric) GreaterOrEqual(value float64) {
// 	v := fmt.Sprintf("'%f'", value)
// 	o := ">="
// 	u := "AND"
// 	n.wo = &o
// 	n.wv = &v
// 	n.wu = &u
// }

// // Returns name for this column.
// func (n *numeric) nme() string {
// 	return n.nm
// }

// // Returns where string.
// func (n *numeric) whr() string {

// 	v := ""

// 	if n.wo != nil && n.wv != nil {
// 		v = n.nm + " " + *n.wo + " " + *n.wv
// 	}

// 	if n.wu != nil {
// 		v += " " + *n.wu
// 	}

// 	return v
// }

// // Returns a pointer to a buffer.
// func (n *numeric) bff() any {
// 	return &n.rw
// }

// // Returns postgresql type for this column.
// func (numeric) pgt() string {
// 	return "NUMERIC"
// }

// /*
// Type "timestamp", corresponds to the type number in postgresql "TIMESTAMP WITHOUT TIME ZONE",
// this type implements the interface "column".
// */
type Time string
type whereTime struct {
	whr *where
}

// type timestamp struct {
// 	nme string     // Name of column.
// 	ptr *time.Time // Pointer to buffer.
// }

// // Creates object type time.Time corresponding in postgresql as timestamp.
// func Time(name string) *timestamp {
// 	return &timestamp{nme: name}
// }

// // Returns name for this column.
// func (t *timestamp) name() string {
// 	return t.nme
// }

// Returns value from buffer.
func (t Time) Row(reader *reader) int64 {
	return *reader.buf[reader.rid[t]].(*int64)
}

// // Creates object type where.
// func (t Time) Where(value time.Time) *where {
// 	return &where{col: t, val: fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond()), opr: "="}
// }

// // Update value for where.
// func (t *Time) WhereValue(where *where, value time.Time) {
// 	if t == where.col {
// 		where.val = fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
// 	} else {
// 		xlog.Fatalln("Cannot update value for *where for another column.")
// 	}
// }

// Creates a sort object with a default value - asc.
// func (t Time) Sort() *order {
// 	return &order{nme: t.nam(), asc: true}
// }

// Returns name for this column.
func (t Time) nam() string {
	return string(t)
}

// Create and returm pointer to buffer.
func (Time) buf() any {
	return new(time.Time)
}

// // Update value for filter.
// func (t *timestamp) Where(value time.Time) {
// 	value = value.UTC()
// 	*t.wv = fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
// }

// // Establishes the logic of the association where.
// func (t *timestamp) And() {
// 	*t.wu = "AND"
// }

// // Establishes the logic of the association where.
// func (t *timestamp) Or() {
// 	*t.wu = "OR"
// }

// // Sets the filter less than for type timestamp.
// func (t *timestamp) LessThan(value time.Time) {
// 	value = value.UTC()
// 	v := fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
// 	o := "<"
// 	u := "AND"
// 	t.wo = &o
// 	t.wv = &v
// 	t.wu = &u
// }

// // Sets the filter less or equal for type timestamp.
// func (t *timestamp) LessOrEqual(value time.Time) {
// 	value = value.UTC()
// 	v := fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
// 	o := "<="
// 	u := "AND"
// 	t.wo = &o
// 	t.wv = &v
// 	t.wu = &u
// }

// // Sets the filter equal for type timestamp.
// func (t *timestamp) Equal(value time.Time) {
// 	value = value.UTC()
// 	v := fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
// 	o := "="
// 	u := "AND"
// 	t.wo = &o
// 	t.wv = &v
// 	t.wu = &u
// }

// // Sets the filter not equal for type timestamp.
// func (t *timestamp) NotEqual(value time.Time) {
// 	value = value.UTC()
// 	v := fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
// 	o := "<>"
// 	u := "AND"
// 	t.wo = &o
// 	t.wv = &v
// 	t.wu = &u
// }

// // Sets the filter greater than for type timestamp.
// func (t *timestamp) GreaterThan(value time.Time) {
// 	value = value.UTC()
// 	v := fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
// 	o := ">"
// 	u := "AND"
// 	t.wo = &o
// 	t.wv = &v
// 	t.wu = &u
// }

// // Sets the filter greater or equal for type timestamp.
// func (t *timestamp) GreaterOrEqual(value time.Time) {
// 	value = value.UTC()
// 	v := fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
// 	o := ">="
// 	u := "AND"
// 	t.wo = &o
// 	t.wv = &v
// 	t.wu = &u
// }

// // Sets sorting by asc.
// func (t *timestamp) Asc() {
// 	v := true
// 	t.sb = &v
// }

// // Sets sorting by desc.
// func (t *timestamp) Desc() {
// 	v := false
// 	t.sb = &v
// }

// // Returns name for this column.
// func (t *timestamp) nme() string {
// 	return t.nm
// }

// // Returns where string.
// func (t *timestamp) whr() string {

// 	v := ""

// 	if t.wo != nil && t.wv != nil {
// 		v = t.nm + " " + *t.wo + " " + *t.wv
// 	}

// 	if t.wu != nil {
// 		v += " " + *t.wu
// 	}

// 	return v
// }

// // Returns a pointer to a buffer.
// func (t *timestamp) bff() any {
// 	return &t.rw
// }

// // Returns postgresql type for this column.
// func (timestamp) pgt() string {
// 	return "TIMESTAMP WITHOUT TIME ZONE"
// }
