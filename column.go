package pdb

import (
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
	Name() string // Returns name for this column.
	Type() string // Returns sql type for this column.
	// Update()
	// value() any

	buffer() []any // Returns the rows from insert buffer.

	size() int    // Returns the len of the buffer.
	pointer() any // Returns a pointer to a buffer variable.
	hook()        // Calling a custom function for select.
}

/*
Type col, this type is the parent of all columns without exception and
partially implements the interface column.
*/

type col struct {
	nam string // Colimn name.
	buf []any  // Rows for inset buffer.
}

// Returns name for this column.
func (c *col) Name() string {
	return c.nam
}

// Returns the len of the buffer.
func (c *col) size() int {
	return len(c.buf)
}

// Returns the rows from insert buffer.
func (c *col) buffer() []any {
	defer c.clear()
	return c.buf
}

// Clear insert buffer.
func (c *col) clear() {
	c.buf = nil
}

/*
Type "Float64", corresponds to the type number in SQL "NUMERIC",
this type is child from type "col" and
partially implements the interface "column".
*/

type numeric struct {
	col                       // Base colimn.
	buf float64               // Row for select buffer.
	row func(numeric float64) // Calling a custom function for select.
}

// Creates object type float64 corresponding in sql as "NUMERIC".
func (t *table) Float64(name string, row func(numeric float64)) *numeric {
	typ := &numeric{col: col{nam: name}, row: row}
	t.addColumn(typ)
	return typ
}

// Returns sql type for this column.
func (numeric) Type() string {
	return "NUMERIC"
}

// Returns a pointer to an object where.
// func (t *text) Where(value float64) *where {
// 	return &where{col: t.nam, val: strconv.FormatFloat(value, 'E', -1, 64)}
// }

// // Updates a value in an object where.
// func (t *text) Update(where *where, value float64) {
// 	where.val = value
// }

// Adds a row to the buffer before inserting.
func (n *numeric) Row(value float64) {
	n.col.buf = append(n.col.buf, value)
}

//	func (n *numeric) value() any {
//		return n.data
//	}
//
//	func (n *numeric) pointer() any {
//		return &n.data
//	}
// func (n *numeric) buffer() []any {
// 	defer n.clearBuffer()
// 	return n.rows
// }
// func (n *numeric) clearBuffer() {
// 	n.rows = nil
// }

// Returns a pointer to a buffer variable.
func (n *numeric) pointer() any {
	return &n.buf
}

func (n *numeric) hook() {
	if n.row != nil {
		n.row(n.buf)
	}
}

/*
Type "Int64", corresponds to the type number in SQL "INT8",
this type is child from type "col" and
partially implements the interface "column".
*/

type bigint struct {
	col                   // Base colimn.
	buf int64             // Row for select buffer.
	row func(value int64) // Calling a custom function for select.
}

// Creates object type int64 corresponding in sql as int8.
func (t *table) Int64(name string, row func(value int64)) *bigint {
	typ := &bigint{col: col{nam: name}, row: row}
	t.addColumn(typ)
	return typ
}

// Returns sql type for this column.
func (bigint) Type() string {
	return "INT8"
}

// Returns a pointer to an object where.
// func (b *bigint) Where(value int64) *where {
// 	return &where{col: b.nam, val: strconv.FormatInt(value, 10)}
// }

// Updates a value in an object where.
// func (b *bigint) Update(where *where, value int64) {
// 	where.val = strconv.FormatInt(value, 10)
// }

// Adds a row to the buffer before inserting.
func (b *bigint) Row(value int64) {
	b.col.buf = append(b.col.buf, value)
}

//	func (b *bigint) value() any {
//		return b.data
//	}
//
//	func (b *bigint) pointer() any {
//		return &b.data
//	}
// func (b *bigint) buffer() []any {
// 	defer b.clearBuffer()
// 	return b.rows
// }
// func (b *bigint) clearBuffer() {
// 	b.rows = nil
// }

// Returns a pointer to a buffer variable.
func (b *bigint) pointer() any {
	return &b.buf
}

func (b *bigint) hook() {
	if b.row != nil {
		b.row(b.buf)
	}
}

/*
Type "String", corresponds to the type number in SQL "TEXT",
this type is child from type "col" and
partially implements the interface "column".
*/

type text struct {
	col                    // Base colimn.
	buf string             // Row for select buffer.
	row func(value string) // Calling a custom function for select.
}

// Creates object type string corresponding in sql as text.
func (t *table) String(name string, row func(value string)) *text {
	typ := &text{col: col{nam: name}, row: row}
	t.addColumn(typ)
	return typ
}

// Returns sql type for this column.
func (text) Type() string {
	return "TEXT"
}

// Returns a pointer to an object where.
// func (t *text) Where(value string) *where {
// 	return &where{col: t.nam, val: value}
// }

// Updates a value in an object where.
// func (t *text) Update(where *where, value string) {
// 	where.val = value
// }

// Adds a row to the buffer before inserting.
func (t *text) Row(value string) {
	t.col.buf = append(t.col.buf, value)
}

// Returns a pointer to object where..
// func (t *text) Where(value string) *where {
// 	return &where{val: value}
// }

// func (t *text) WhereUpdate(value string) *where {
// 	return &where{val: value}
// }

//	func (t *text) value() any {
//		return t.data
//	}
//
//	func (t *text) pointer() any {
//		return &t.data
//	}
// func (t *text) buffer() []any {
// 	defer t.clearBuffer()
// 	return t.rows
// }
// func (t *text) clearBuffer() {
// 	t.rows = nil
// }

// Returns a pointer to a buffer variable.
func (t *text) pointer() any {
	return &t.buf
}

func (t *text) hook() {
	if t.row != nil {
		t.row(t.buf)
	}
}

/*
Type "Time", corresponds to the type number in SQL "TIMESTAMP",
this type is child from type "col" and
partially implements the interface "column".
*/

type timestamp struct {
	col                       // Base colimn.
	buf time.Time             // Row for select buffer.
	row func(value time.Time) // Calling a custom function for select.
}

// Creates object type time.Time corresponding in sql as timestamp.
func (t *table) Time(name string, row func(value time.Time)) *timestamp {
	typ := &timestamp{col: col{nam: name}, row: row}
	t.addColumn(typ)
	return typ
}

// Returns sql type for this column.
func (timestamp) Type() string {
	return "TIMESTAMP WITHOUT TIME ZONE"
}

// // Returns a pointer to an object where.
// func (t *timestamp) Where(value time.Time) *where {
// 	return &where{col: t.nam, val: value.String()}
// }

// // Updates a value in an object where.
// func (t *timestamp) Update(where *where, value time.Time) {
// 	where.val = value.String()
// }

// Adds a row to the buffer before inserting.
func (t *timestamp) Row(value time.Time) {
	t.col.buf = append(t.col.buf, value.UTC())
}

//	func (t *timestamp) value() any {
//		return t.data
//	}
//
//	func (t *timestamp) pointer() any {
//		return &t.data
//	}
//
//	func (t *timestamp) buffer() []any {
//		defer t.clearBuffer()
//		return t.rows
//	}
// func (t *timestamp) clearBuffer() {
// 	t.rows = nil
// }

// Returns a pointer to a buffer variable.
func (t *timestamp) pointer() any {
	return &t.buf
}

func (t *timestamp) hook() {
	if t.row != nil {
		t.row(t.buf.UTC())
	}
}
