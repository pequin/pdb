package pdb

import (
	"fmt"
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
	nam() string // Returns name of the column.
	buf() any    // Creates pointer to buffer and and returns pointer to it.
}

/*
Type "Bool", corresponds to the type number in postgresql "BOOLEAN",
this type implements the interface "column".
*/
type Bool string
type whereBool struct {
	*where
}

// Returns value from buffer.
func (b Bool) Row(reader *reader) bool {

	idx, ise := reader.idx[b]
	if !ise {
		xlog.Fatallf("The column \"%s\" is not associated with the reader.", b.nam())
	}
	return *reader.buf[idx].(*bool)
}

// Makes new object whereString and returns pointer to it.
func (b *Bool) Where() *whereBool {
	return &whereBool{where: &where{col: b}}
}

// Returns name for this column.
func (b Bool) nam() string {
	return string(b)
}

// Creates and returms pointer to buffer.
func (Bool) buf() any {
	return new(bool)
}

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
type String string
type whereString struct {
	*where
}

// Returns value from buffer.
func (s String) Row(reader *reader) string {

	idx, ise := reader.idx[s]
	if !ise {
		xlog.Fatallf("The column \"%s\" is not associated with the reader.", s.nam())
	}
	return *reader.buf[idx].(*string)
}

// Makes new object whereString and returns pointer to it.
func (s *String) Where() *whereString {
	return &whereString{where: &where{col: s}}
}

// Returns name for this column.
func (s String) nam() string {
	return string(s)
}

// Creates and returms pointer to buffer.
func (String) buf() any {
	return new(string)
}

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
type Int64 string
type whereInt64 struct {
	*where
}

// Returns value from buffer.
func (i Int64) Row(reader *reader) int64 {

	idx, ise := reader.idx[i]
	if !ise {
		xlog.Fatallf("The column \"%s\" is not associated with the reader.", i.nam())
	}
	return *reader.buf[idx].(*int64)
}

// Makes new object whereByInt64 and returns pointer to it.
func (i *Int64) Where() *whereInt64 {
	return &whereInt64{where: &where{col: i}}
}

// Returns name for this column.
func (i Int64) nam() string {
	return string(i)
}

// Creates and returms pointer to buffer.
func (Int64) buf() any {
	return new(int64)
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
type Float64 string
type whereFloat64 struct {
	*where
}

// Returns value from buffer.
func (f Float64) Row(reader *reader) float64 {

	idx, ise := reader.idx[f]
	if !ise {
		xlog.Fatallf("The column \"%s\" is not associated with the reader.", f.nam())
	}
	return *reader.buf[idx].(*float64)
}

// Makes new object whereFloat64 and returns pointer to it.
func (f *Float64) Where() *whereFloat64 {
	return &whereFloat64{where: &where{col: f}}
}

// Returns name for this column.
func (f Float64) nam() string {
	return string(f)
}

// Create and returms pointer to buffer.
func (Float64) buf() any {
	return new(float64)
}

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
type Time string
type whereTime struct {
	*where
}

// Returns value from buffer.
func (t Time) Row(reader *reader) time.Time {

	idx, ise := reader.idx[t]
	if !ise {
		xlog.Fatallf("The column \"%s\" is not associated with the reader.", t.nam())
	}
	return *reader.buf[idx].(*time.Time)
}

// Makes new object whereTime and returns pointer to it.
func (t *Time) Where() *whereTime {
	return &whereTime{where: &where{col: t}}
}

// Returns name for this column.
func (t Time) nam() string {
	return string(t)
}

// Create and returms pointer to buffer.
func (Time) buf() any {
	return new(time.Time)
}

// Seteds operator "<=" for custom value and returns pointer to object where.
func (w *whereTime) LessOrEqual(value time.Time) *where {
	w.where.opr = "<="
	w.where.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.where
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (w *whereTime) Equal(value time.Time) *where {
	w.where.opr = "="
	w.where.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.where
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (w *whereTime) NotEqual(value time.Time) *where {
	w.where.opr = "<>"
	w.where.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.where
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (w *whereTime) Greater(value time.Time) *where {
	w.where.opr = ">"
	w.where.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.where
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (w *whereTime) GreaterOrEqual(value time.Time) *where {
	w.where.opr = ">="
	w.where.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.where
}

// Updates custom value.
func (w *whereTime) Value(v time.Time) {
	w.val = fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", v.Year(), v.Month(), v.Day(), v.Hour(), v.Minute(), v.Second(), v.Nanosecond())
}
