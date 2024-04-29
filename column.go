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

type column interface {
	table() *table
	name() string  // Returns name for this column.
	primary() bool // Returns true if this column is as primary.
	sql() string   // Returns sql type for this column.
	pointer() any  // Returns a pointer to a buffer variable.
}

/*
Type col, this type is the parent of all columns without exception and
partially implements the interface column.
*/

type col struct {
	nam string // Column name.
	pri bool   // Is as primary.
	tab *table // Table.
}

// Returns the associated table.
func (c *col) table() *table {
	return c.tab
}

// Returns name for this column.
func (c *col) name() string {
	return c.nam
}

// Returns true if this column is as primary.
func (c *col) primary() bool {
	return c.pri
}

/*
Type "Float64", corresponds to the type number in SQL "NUMERIC",
this type is child from type "col" and
partially implements the interface "column".
*/

type numeric struct {
	Row float64 // Row for select buffer.
	col         // Base column.
}

// Creates object type float64 corresponding in sql as "NUMERIC".
func (t *table) Float64(name string, primary bool) *numeric {
	typ := &numeric{col: col{nam: name, pri: primary, tab: t}}
	t.associate(typ)
	return typ
}

// Adds a row to the buffer before inserting.
func (n *numeric) Insert(value float64) {
	n.tab.insert(n, value)
}

// Updates the value in a column.
func (n *numeric) Update(value float64, where *where) {

	whe := ""
	if where != nil {
		whe = " " + where.sql()
	}
	_, err := n.tab.sch.dat.trx.Exec(fmt.Sprintf("UPDATE %s.%s SET %s = %s%s;", n.tab.sch.nam, n.tab.nam, n.nam, n.format(value), whe))
	xlog.Fatalln(err)
}

// Order by asc.
func (n *numeric) Asc(columns ...column) *order {
	o := &order{asc: true, tab: n.tab}
	o.add(n)
	o.add(columns...)
	return o
}

// Order by desc.
func (n *numeric) Desc(columns ...column) *order {
	o := &order{asc: false, tab: n.tab}
	o.add(n)
	o.add(columns...)
	return o
}

// Returns a pointer to object where with operator "=" as equal.
func (n *numeric) Equal(value float64) *where {
	return &where{col: n, ope: "=", val: n.format(value)}
}

// Returns a pointer to object where with operator "IN".
func (n *numeric) In(values ...float64) *where {

	str := make([]string, len(values))

	for i := 0; i < len(values); i++ {
		str[i] = n.format(values[i])
	}

	return &where{col: n, ope: "IN", val: "(" + strings.Join(str, ", ") + ")"}
}

// Returns a pointer to object where with operator ">" 	greater than.
func (n *numeric) Greater(value float64) *where {
	return &where{col: n, ope: ">", val: n.format(value)}
}

// Returns a pointer to object where with operator "<" less than.
func (n *numeric) Less(value float64) *where {
	return &where{col: n, ope: "<", val: n.format(value)}
}

// Returns a pointer to object where with operator ">=" greater than or equal.
func (n *numeric) GreaterOrEqual(value float64) *where {
	return &where{col: n, ope: ">=", val: n.format(value)}
}

// Returns a pointer to object where with operator "<=" less than or equal.
func (n *numeric) LessOrEqual(value float64) *where {
	return &where{col: n, ope: "<=", val: n.format(value)}
}

// Returns a pointer to object where with operator "<> or !=" not equal.
func (n *numeric) NotEqual(value float64) *where {
	return &where{col: n, ope: "<> or !=", val: n.format(value)}
}

func (n *numeric) format(value float64) string {
	return fmt.Sprintf("'%f'", value)
}

// Returns sql type for this column.
func (numeric) sql() string {
	return "NUMERIC"
}

// Returns a pointer to a buffer variable.
func (n *numeric) pointer() any {
	return &n.Row
}

/*
Type "Int64", corresponds to the type number in SQL "INT8",
this type is child from type "col" and
partially implements the interface "column".
*/

type bigint struct {
	Row int64 // Row for select buffer.
	col       // Base column.
}

// Creates object type int64 corresponding in sql as int8.
func (t *table) Int64(name string, primary bool) *bigint {
	typ := &bigint{col: col{nam: name, pri: primary, tab: t}}
	t.associate(typ)
	return typ
}

// Adds a row to the buffer before inserting.
func (b *bigint) Insert(value int64) {
	b.tab.insert(b, value)
}

// Updates the value in a column.
func (b *bigint) Update(value int64, where *where) {

	whe := ""
	if where != nil {
		whe = " " + where.sql()
	}
	_, err := b.tab.sch.dat.trx.Exec(fmt.Sprintf("UPDATE %s.%s SET %s = %s%s;", b.tab.sch.nam, b.tab.nam, b.nam, b.format(value), whe))
	xlog.Fatalln(err)
}

// Order by asc.
func (b *bigint) Asc(columns ...column) *order {
	o := &order{asc: true, tab: b.tab}
	o.add(b)
	o.add(columns...)
	return o
}

// Order by desc.
func (b *bigint) Desc(columns ...column) *order {
	o := &order{asc: false, tab: b.tab}
	o.add(b)
	o.add(columns...)
	return o
}

// Returns a pointer to object where with operator "=" as equal.
func (b *bigint) Equal(value int64) *where {
	return &where{col: b, ope: "=", val: b.format(value)}
}

// Returns a pointer to object where with operator "IN".
func (b *bigint) In(values ...int64) *where {

	str := make([]string, len(values))

	for i := 0; i < len(values); i++ {
		str[i] = b.format(values[i])
	}

	return &where{col: b, ope: "IN", val: "(" + strings.Join(str, ", ") + ")"}
}

// Returns a pointer to object where with operator ">" 	greater than.
func (b *bigint) Greater(value int64) *where {
	return &where{col: b, ope: ">", val: b.format(value)}
}

// Returns a pointer to object where with operator "<" less than.
func (b *bigint) Less(value int64) *where {
	return &where{col: b, ope: "<", val: b.format(value)}
}

// Returns a pointer to object where with operator ">=" greater than or equal.
func (b *bigint) GreaterOrEqual(value int64) *where {
	return &where{col: b, ope: ">=", val: b.format(value)}
}

// Returns a pointer to object where with operator "<=" less than or equal.
func (b *bigint) LessOrEqual(value int64) *where {
	return &where{col: b, ope: "<=", val: b.format(value)}
}

// Returns a pointer to object where with operator "<> or !=" not equal.
func (b *bigint) NotEqual(value int64) *where {
	return &where{col: b, ope: "<> or !=", val: b.format(value)}
}

func (b *bigint) format(value int64) string {
	return fmt.Sprintf("'%d'", value)
}

// Returns sql type for this column.
func (bigint) sql() string {
	return "INT8"
}

// Returns a pointer to a buffer variable.
func (b *bigint) pointer() any {
	return &b.Row
}

/*
Type "String", corresponds to the type number in SQL "TEXT",
this type is child from type "col" and
partially implements the interface "column".
*/

type text struct {
	Row string // Row for select buffer.
	col        // Base column.
}

// Creates object type string corresponding in sql as text.
func (t *table) String(name string, primary bool) *text {
	typ := &text{col: col{nam: name, pri: primary, tab: t}}
	t.associate(typ)
	return typ
}

// Adds a row to the buffer before inserting.
func (t *text) Insert(value string) {
	t.tab.insert(t, value)
}

// Updates the value in a column.
func (t *text) Update(value string, where *where) {

	whe := ""
	if where != nil {
		whe = " " + where.sql()
	}
	_, err := t.tab.sch.dat.trx.Exec(fmt.Sprintf("UPDATE %s.%s SET %s = %s%s;", t.tab.sch.nam, t.tab.nam, t.nam, t.format(value), whe))
	xlog.Fatalln(err)
}

// Order by asc.
func (t *text) Asc(columns ...column) *order {
	o := &order{asc: true, tab: t.tab}
	o.add(t)
	o.add(columns...)
	return o
}

// Order by desc.
func (t *text) Desc(columns ...column) *order {
	o := &order{asc: false, tab: t.tab}
	o.add(t)
	o.add(columns...)
	return o
}

// Returns a pointer to object where with operator "=" as equal.
func (t *text) Equal(value string) *where {
	return &where{col: t, ope: "=", val: t.format(value)}
}

// Returns a pointer to object where with operator "IN".
func (t *text) In(values ...string) *where {

	str := make([]string, len(values))

	for i := 0; i < len(values); i++ {
		str[i] = t.format(values[i])
	}

	return &where{col: t, ope: "IN", val: "(" + strings.Join(str, ", ") + ")"}
}

// Returns a pointer to object where with operator ">" 	greater than.
func (t *text) Greater(value string) *where {
	return &where{col: t, ope: ">", val: t.format(value)}
}

// Returns a pointer to object where with operator "<" less than.
func (t *text) Less(value string) *where {
	return &where{col: t, ope: "<", val: t.format(value)}
}

// Returns a pointer to object where with operator ">=" greater than or equal.
func (t *text) GreaterOrEqual(value string) *where {
	return &where{col: t, ope: ">=", val: t.format(value)}
}

// Returns a pointer to object where with operator "<=" less than or equal.
func (t *text) LessOrEqual(value string) *where {
	return &where{col: t, ope: "<=", val: t.format(value)}
}

// Returns a pointer to object where with operator "<> or !=" not equal.
func (t *text) NotEqual(value string) *where {
	return &where{col: t, ope: "<> or !=", val: t.format(value)}
}

func (t *text) format(value string) string {
	return fmt.Sprintf("'%s'", value)
}

// Returns sql type for this column.
func (text) sql() string {
	return "TEXT"
}

// Returns a pointer to a buffer variable.
func (t *text) pointer() any {
	return &t.Row
}

/*
Type "Time", corresponds to the type number in SQL "TIMESTAMP",
this type is child from type "col" and
partially implements the interface "column".
*/

type timestamp struct {
	Row time.Time // Row for select buffer.
	col           // Base column.
}

// Creates object type time.Time corresponding in sql as timestamp.
func (t *table) Time(name string, primary bool) *timestamp {
	typ := &timestamp{col: col{nam: name, pri: primary, tab: t}}
	t.associate(typ)
	return typ
}

// Adds a row to the buffer before inserting.
func (t *timestamp) Insert(value time.Time) {
	t.tab.insert(t, t.format(value.UTC()))
}

// Updates the value in a column.
func (t *timestamp) Update(value time.Time, where *where) {

	whe := ""
	if where != nil {
		whe = " " + where.sql()
	}
	_, err := t.tab.sch.dat.trx.Exec(fmt.Sprintf("UPDATE %s.%s SET %s = %s%s;", t.tab.sch.nam, t.tab.nam, t.nam, t.format(value), whe))
	xlog.Fatalln(err)
}

// Returns a pointer to object where with operator "=" as equal.
func (t *timestamp) Equal(value time.Time) *where {
	return &where{col: t, ope: "=", val: t.format(value)}
}

// Returns a pointer to object where with operator "IN".
func (t *timestamp) In(values ...time.Time) *where {

	str := make([]string, len(values))

	for i := 0; i < len(values); i++ {
		str[i] = t.format(values[i])
	}

	return &where{col: t, ope: "IN", val: "(" + strings.Join(str, ", ") + ")"}
}

// Returns a pointer to object where with operator ">" 	greater than.
func (t *timestamp) Greater(value time.Time) *where {
	return &where{col: t, ope: ">", val: t.format(value)}
}

// Returns a pointer to object where with operator "<" less than.
func (t *timestamp) Less(value time.Time) *where {
	return &where{col: t, ope: "<", val: t.format(value)}
}

// Returns a pointer to object where with operator ">=" greater than or equal.
func (t *timestamp) GreaterOrEqual(value time.Time) *where {
	return &where{col: t, ope: ">=", val: t.format(value)}
}

// Returns a pointer to object where with operator "<=" less than or equal.
func (t *timestamp) LessOrEqual(value time.Time) *where {
	return &where{col: t, ope: "<=", val: t.format(value)}
}

// Returns a pointer to object where with operator "<> or !=" not equal.
func (t *timestamp) NotEqual(value time.Time) *where {
	return &where{col: t, ope: "<> or !=", val: t.format(value)}
}

func (t *timestamp) format(value time.Time) string {
	return fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
}

// Returns sql type for this column.
func (timestamp) sql() string {
	return "TIMESTAMP WITHOUT TIME ZONE"
}

// Returns a pointer to a buffer variable.
func (t *timestamp) pointer() any {
	return &t.Row
}
