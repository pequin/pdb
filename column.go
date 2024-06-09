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
	buf() any    // Create and returm pointer to buffer.
}

/*
Type "Bool", corresponds to the type number in postgresql "BOOLEAN",
this type implements the interface "column".
*/
type Bool string
type whereBool struct {
	whr *where
}

// Returns value from buffer.
func (b Bool) Row(reader *reader) bool {
	return *reader.buf[reader.rid[b]].(*bool)
}

// Creates new object whereBool and return pointer to it.
func (b Bool) Where() *whereBool {
	return &whereBool{whr: &where{col: b}}
}

func (w *whereBool) Equal(value bool) *where {
	w.whr.opr = "="
	w.whr.val = fmt.Sprintf("%t", value)
	return w.whr
}
func (w *whereBool) NotEqual(value bool) *where {
	w.whr.opr = "<>"
	w.whr.val = fmt.Sprintf("%t", value)
	return w.whr
}

// Returns name for this column.
func (b Bool) nam() string {
	return string(b)
}

// Creates and returm pointer to buffer.
func (Bool) buf() any {
	return new(bool)
}

/*
Type "String", corresponds to the type number in postgresql "TEXT",
this type implements the interface "column".
*/
type String string
type whereString struct {
	whr *where
}

// Returns value from buffer.
func (s String) Row(reader *reader) string {
	return *reader.buf[reader.rid[s]].(*string)
}

// Creates new object whereString and return pointer to it.
func (s String) Where() *whereString {
	return &whereString{whr: &where{col: s}}
}

func (w *whereString) Less(value string) *where {
	w.whr.opr = "<"
	w.whr.val = value
	return w.whr
}
func (w *whereString) LessOrEqual(value string) *where {
	w.whr.opr = "<="
	w.whr.val = value
	return w.whr
}
func (w *whereString) Equal(value string) *where {
	w.whr.opr = "="
	w.whr.val = value
	return w.whr
}
func (w *whereString) NotEqual(value string) *where {
	w.whr.opr = "<>"
	w.whr.val = value
	return w.whr
}
func (w *whereString) Greater(value string) *where {
	w.whr.opr = ">"
	w.whr.val = value
	return w.whr
}
func (w *whereString) GreaterOrEqual(value string) *where {
	w.whr.opr = ">="
	w.whr.val = value
	return w.whr
}

// Returns name for this column.
func (s String) nam() string {
	return string(s)
}

// Creates and returm pointer to buffer.
func (String) buf() any {
	return new(string)
}

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

// Creates new object whereInt64 and return pointer to it.
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

// Returns name for this column.
func (i Int64) nam() string {
	return string(i)
}

// Creates and returm pointer to buffer.
func (Int64) buf() any {
	return new(int64)
}

/*
Type "Float64", corresponds to the type number in postgresql "NUMERIC",
this type implements the interface "column".
*/
type Float64 string
type whereFloat64 struct {
	whr *where
}

// Returns value from buffer.
func (f Float64) Row(reader *reader) float64 {
	return *reader.buf[reader.rid[f]].(*float64)
}

// Creates new object whereFloat64 and return pointer to it.
func (f Float64) Where() *whereFloat64 {
	return &whereFloat64{whr: &where{col: f}}
}

func (w *whereFloat64) Less(value float64) *where {
	w.whr.opr = "<"
	w.whr.val = fmt.Sprintf("%f", value)
	return w.whr
}
func (w *whereFloat64) LessOrEqual(value float64) *where {
	w.whr.opr = "<="
	w.whr.val = fmt.Sprintf("%f", value)
	return w.whr
}
func (w *whereFloat64) Equal(value float64) *where {
	w.whr.opr = "="
	w.whr.val = fmt.Sprintf("%f", value)
	return w.whr
}
func (w *whereFloat64) NotEqual(value float64) *where {
	w.whr.opr = "<>"
	w.whr.val = fmt.Sprintf("%f", value)
	return w.whr
}
func (w *whereFloat64) Greater(value float64) *where {
	w.whr.opr = ">"
	w.whr.val = fmt.Sprintf("%f", value)
	return w.whr
}
func (w *whereFloat64) GreaterOrEqual(value float64) *where {
	w.whr.opr = ">="
	w.whr.val = fmt.Sprintf("%f", value)
	return w.whr
}

// Returns name for this column.
func (f Float64) nam() string {
	return string(f)
}

// Create and returm pointer to buffer.
func (Float64) buf() any {
	return new(float64)
}

/*
Type "Time", corresponds to the type number in postgresql "TIMESTAMP WITHOUT TIME ZONE",
this type implements the interface "column".
*/
type Time string
type whereTime struct {
	whr *where
}

// Returns value from buffer.
func (t Time) Row(reader *reader) time.Time {
	return *reader.buf[reader.rid[t]].(*time.Time)
}

// Creates new object whereTime and return pointer to it.
func (t Time) Where() *whereTime {
	return &whereTime{whr: &where{col: t}}
}

func (w *whereTime) Less(value time.Time) *where {
	w.whr.opr = "<"
	w.whr.val = fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.whr
}
func (w *whereTime) LessOrEqual(value time.Time) *where {
	w.whr.opr = "<="
	w.whr.val = fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.whr
}
func (w *whereTime) Equal(value time.Time) *where {
	w.whr.opr = "="
	w.whr.val = fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.whr
}
func (w *whereTime) NotEqual(value time.Time) *where {
	w.whr.opr = "<>"
	w.whr.val = fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.whr
}
func (w *whereTime) Greater(value time.Time) *where {
	w.whr.opr = ">"
	w.whr.val = fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.whr
}
func (w *whereTime) GreaterOrEqual(value time.Time) *where {
	w.whr.opr = ">="
	w.whr.val = fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
	return w.whr
}

// Returns name for this column.
func (t Time) nam() string {
	return string(t)
}

// Create and returm pointer to buffer.
func (Time) buf() any {
	return new(time.Time)
}
