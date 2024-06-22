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
}

type column struct {
	tbl *table
	nme string // Name.
	pry bool   // Is primary.
	idx bool   // Is indexed.
}

func (c *column) update(value any, filer *filter) {

	if value != nil {
		_, err := c.tbl.stx.Exec(fmt.Sprintf("UPDATE %s SET %s = $1%s;", c.tbl.name(), c.nme, filer.where(c.tbl)), value)
		xlog.Fatalln(err)
	}
}

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

// The first argument is a name of new column, and the value returned is a pointer to a column type boolean newly associated with this table.
func (t *types) Bool(name string) *boolean {
	typ := &boolean{column{tbl: t.tbl, nme: name, pry: false, idx: false}}
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

// The first argument is a name of new column, and the value returned is a pointer to a column type text newly associated with this table.
func (t *types) String(name string) *text {
	typ := &text{column{tbl: t.tbl, nme: name, pry: false, idx: false}}
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

// Type boolean, corresponds to the type in postgresql BOOLEAN and implements the interface a column.
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

// Updates the values ​​in a column.
func (b *boolean) Update(value bool, filer *filter) {
	b.update(value, filer)
}

// Makes new object indexBoolean and returns pointer to it.
func (b *boolean) NewIndex() *indexBoolean {
	b.idx = true
	return &indexBoolean{idx: &index{nme: b.name()}}
}

// Seteds operator "=" for custom value and returns pointer to object indexBoolean.
func (i *indexBoolean) Equal(value bool) *index {
	return i.idx.equal(fmt.Sprintf("%t", value))
}

// Seteds operator "<>" for custom value and returns pointer to object indexBoolean.
func (i *indexBoolean) NotEqual(value bool) *index {
	return i.idx.notEqual(fmt.Sprintf("%t", value))
}

// Returns postgresql type.
func (boolean) sql() string {
	return "BOOLEAN"
}

// Type bigint, corresponds to the type in postgresql TEXT and implements the interface a column.
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

// Updates the values ​​in a column.
func (b *bigint) Update(value int64, filer *filter) {
	b.update(value, filer)
}

// Set as primary.
func (b *bigint) AsPrimary() *bigint {
	b.pry = true
	return b
}

// Makes new object indexBigint and returns pointer to it.
func (b *bigint) NewIndex() *indexBigint {
	b.idx = true
	return &indexBigint{idx: &index{nme: b.name()}}
}

// Seteds operator "<" for custom value and returns pointer to object where.
func (i *indexBigint) Less(value int64) *index {
	return i.idx.less(fmt.Sprintf("%d", value))
}

// Seteds operator "<=" for custom value and returns pointer to object where.
func (i *indexBigint) LessOrEqual(value int64) *index {
	return i.idx.lessOrEqual(fmt.Sprintf("%d", value))
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (i *indexBigint) Equal(value int64) *index {
	return i.idx.equal(fmt.Sprintf("%d", value))
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (i *indexBigint) NotEqual(value int64) *index {
	return i.idx.notEqual(fmt.Sprintf("%d", value))
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (i *indexBigint) Greater(value int64) *index {
	return i.idx.greater(fmt.Sprintf("%d", value))
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (i *indexBigint) GreaterOrEqual(value int64) *index {
	return i.idx.greaterOrEqual(fmt.Sprintf("%d", value))
}

// Returns postgresql type.
func (bigint) sql() string {
	return "BIGINT"
}

// Type serial, corresponds to the type in postgresql BIGSERIAL and implements the interface a column.
type serial struct {
	column
}

// Makes new object indexBigint and returns pointer to it.
func (b *serial) NewIndex() *indexBigint {
	b.idx = true
	return &indexBigint{idx: &index{nme: b.name()}}
}

// Returns postgresql type.
func (serial) sql() string {
	return "BIGSERIAL"
}

// Type numeric, corresponds to the type in postgresql NUMERIC and implements the interface a column.
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

// Updates the values ​​in a column.
func (n *numeric) Update(value float64, filer *filter) {
	n.update(value, filer)
}

// Set as primary.
func (n *numeric) AsPrimary() *numeric {
	n.pry = true
	return n
}

// Makes new object indexNumeric and returns pointer to it.
func (n *numeric) NewIndex() *indexNumeric {
	n.idx = true
	return &indexNumeric{idx: &index{nme: n.name()}}
}

// Seteds operator "<" for custom value and returns pointer to object where.
func (i *indexNumeric) Less(value float64) *index {
	return i.idx.less(fmt.Sprintf("%f", value))
}

// Seteds operator "<=" for custom value and returns pointer to object where.
func (i *indexNumeric) LessOrEqual(value float64) *index {
	return i.idx.lessOrEqual(fmt.Sprintf("%f", value))
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (i *indexNumeric) Equal(value float64) *index {
	return i.idx.equal(fmt.Sprintf("%f", value))
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (i *indexNumeric) NotEqual(value float64) *index {
	return i.idx.notEqual(fmt.Sprintf("%f", value))
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (i *indexNumeric) Greater(value float64) *index {
	return i.idx.greater(fmt.Sprintf("%f", value))
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (i *indexNumeric) GreaterOrEqual(value float64) *index {
	return i.idx.greaterOrEqual(fmt.Sprintf("%f", value))
}

// Returns postgresql type.
func (numeric) sql() string {
	return "NUMERIC"
}

// Type text, corresponds to the type in postgresql TEXT and implements the interface a column.
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

// Updates the values ​​in a column.
func (t *text) Update(value string, filer *filter) {
	t.update(value, filer)
}

// Set as primary.
func (t *text) AsPrimary() *text {
	t.pry = true
	return t
}

// Makes new object indexText and returns pointer to it.
func (t *text) NewIndex() *indexText {
	t.idx = true
	return &indexText{idx: &index{nme: t.name()}}
}

// Seteds operator "<" for custom value and returns pointer to object where.
func (i *indexText) Less(value string) *index {
	return i.idx.less(value)
}

// Seteds operator "<=" for custom value and returns pointer to object where.
func (i *indexText) LessOrEqual(value string) *index {
	return i.idx.lessOrEqual(value)
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (i *indexText) Equal(value string) *index {
	return i.idx.equal(value)
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (i *indexText) NotEqual(value string) *index {
	return i.idx.notEqual(value)
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (i *indexText) Greater(value string) *index {
	return i.idx.greater(value)
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (i *indexText) GreaterOrEqual(value string) *index {
	return i.idx.greaterOrEqual(value)
}

// Returns postgresql type.
func (text) sql() string {
	return "TEXT"
}

// Type timestamp, corresponds to the type in postgresql TIMESTAMP WITHOUT TIME ZONE and implements the interface a column.
type timestamp struct {
	column
}
type indexTimestamp struct {
	idx *index
}

// Write value to a row buffer.
func (t *timestamp) Write(value time.Time) {
	t.tbl.write(t, value.UTC())
}

// Updates the values ​​in a column.
func (t *timestamp) Update(value time.Time, filer *filter) {
	t.update(value.UTC(), filer)
}

// Set as primary.
func (t *timestamp) AsPrimary() *timestamp {
	t.pry = true
	return t
}

// Makes new object indexTimestamp and returns pointer to it.
func (t *timestamp) NewIndex() *indexTimestamp {
	t.idx = true
	return &indexTimestamp{idx: &index{nme: t.name()}}
}

// Seteds operator "<" for custom value and returns pointer to object where.
func (i *indexTimestamp) Less(value time.Time) *index {
	value = value.UTC()
	return i.idx.less(fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond()))
}

// Seteds operator "<=" for custom value and returns pointer to object where.
func (i *indexTimestamp) LessOrEqual(value time.Time) *index {
	value = value.UTC()
	return i.idx.lessOrEqual(fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond()))
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (i *indexTimestamp) Equal(value time.Time) *index {
	value = value.UTC()
	return i.idx.equal(fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond()))
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (i *indexTimestamp) NotEqual(value time.Time) *index {
	value = value.UTC()
	return i.idx.notEqual(fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond()))
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (i *indexTimestamp) Greater(value time.Time) *index {
	value = value.UTC()
	return i.idx.greater(fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond()))
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (i *indexTimestamp) GreaterOrEqual(value time.Time) *index {
	value = value.UTC()
	return i.idx.greaterOrEqual(fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond()))
}

// Returns postgresql type.
func (timestamp) sql() string {
	return "TIMESTAMP WITHOUT TIME ZONE"
}
