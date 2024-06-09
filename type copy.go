package pdb

// /*
// Copyright 2024 Vasiliy Vdovin

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */

// type column interface {
// 	nme() string // Returns name for this column.
// 	pmy() bool   // Returns true if this column is as primary.
// 	uqe() bool   // Returns true if this column is as unique.
// 	pgt() string // Returns sql type for this column.
// 	// pointer() any  // Returns a pointer to a buffer variable.
// }

// /*
// Type "boolean", corresponds to the type number in postgresql "BOOLEAN",
// this type is child from type "col" and
// partially implements the interface "column".
// */
// type boolean struct {
// 	nm string // Name.
// 	rw bool   // Row for select buffer.
// }

// // Creates object type bool corresponding in postgresql as boolean.
// func Bool(name string) *boolean {
// 	return &boolean{nm: name}
// }

// // Returns name for this column.
// func (b *boolean) nme() string {
// 	return b.nm
// }

// // Returns postgresql type for this column.
// func (boolean) pgt() string {
// 	return "BOOLEAN"
// }

// /*
// Type "boolean", corresponds to the type number in SQL "BOOLEAN",
// this type is child from type "col" and
// partially implements the interface "column".
// */

// type text struct {
// 	nm string // Name.
// 	rw bool   // Row for select buffer.
// }

// // Creates object type string corresponding in postgresql as text.
// func String(name string) *text {
// 	return &text{nm: name}
// }

// // Returns name for this column.
// func (t *text) nme() string {
// 	return t.nm
// }

// // Returns postgresql type for this column.
// func (text) pgt() string {
// 	return "TEXT"
// }

// // /*
// // Type "Int64", corresponds to the type number in SQL "INT8",
// // this type is child from type "col" and
// // partially implements the interface "column".
// // */

// // type Int64 struct {
// // 	Row int64 // Row for select buffer.
// // 	col       // Base column.
// // }

// // // Creates object type int64 corresponding in sql as int8.
// // func (t *Table) Int64(name string, primary bool) *Int64 {
// // 	typ := &Int64{col: col{nam: name, pri: primary, tab: t}}
// // 	t.associate(typ)
// // 	return typ
// // }

// // // Adds a row to the buffer before inserting.
// // func (i *Int64) Insert(value int64) {
// // 	i.tab.insert(i, value)
// // }

// // // Updates the value in a column.
// // func (b *Int64) Update(value int64, where *where) {

// // 	whe := ""
// // 	if where != nil {
// // 		whe = " " + where.sql()
// // 	}
// // 	_, err := b.tab.sch.dat.trx.Exec(fmt.Sprintf("UPDATE %s.%s SET %s = %s%s;", b.tab.sch.nam, b.tab.nam, b.nam, b.format(value), whe))
// // 	xlog.Fatalln(err)
// // }

// // // Order by asc.
// // func (i *Int64) Asc(columns ...column) *order {
// // 	o := &order{asc: true, tab: i.tab}
// // 	o.add(i)
// // 	o.add(columns...)
// // 	return o
// // }

// // // Order by desc.
// // func (i *Int64) Desc(columns ...column) *order {
// // 	o := &order{asc: false, tab: i.tab}
// // 	o.add(i)
// // 	o.add(columns...)
// // 	return o
// // }

// /////

// // /*
// // Type col, this type is the parent of all columns without exception and
// // partially implements the interface column.
// // */

// // type col struct {
// // 	nam string // Column name.
// // 	pri bool   // Is as primary.
// // 	tab *Table // Table.
// // }

// // // Returns the associated table.
// // func (c *col) table() *Table {
// // 	return c.tab
// // }

// // // Returns name for this column.
// // func (c *col) name() string {
// // 	return c.nam
// // }

// // // Returns true if this column is as primary.
// // func (c *col) primary() bool {
// // 	return c.pri
// // }

// // type Bool struct {
// // 	Row bool // Row for select buffer.
// // 	col      // Base column.
// // }

// // // Creates object type bool corresponding in sql as timestamp.
// // func (b *Table) Bool(name string, primary bool) *Bool {
// // 	typ := &Bool{col: col{nam: name, pri: primary, tab: b}}
// // 	b.associate(typ)
// // 	return typ
// // }

// // // Adds a row to the buffer before inserting.
// // func (b *Bool) Insert(value bool) {
// // 	b.tab.insert(b, b.format(value))
// // }

// // // Updates the value in a column.
// // func (b *Bool) Update(value bool, where *where) {

// // 	whe := ""
// // 	if where != nil {
// // 		whe = " " + where.sql()
// // 	}
// // 	_, err := b.tab.sch.dat.trx.Exec(fmt.Sprintf("UPDATE %s.%s SET %s = %s%s;", b.tab.sch.nam, b.tab.nam, b.nam, b.format(value), whe))
// // 	xlog.Fatalln(err)
// // }

// // // Returns a pointer to object where with operator "=" as equal.
// // func (b *Bool) Equal(value bool) *where {
// // 	return &where{col: b, ope: "=", val: b.format(value)}
// // }

// // // Returns a pointer to object where with operator "<> or !=" not equal.
// // func (b *Bool) NotEqual(value bool) *where {
// // 	return &where{col: b, ope: "<> or !=", val: b.format(value)}
// // }

// // func (b *Bool) format(value bool) string {
// // 	return strconv.FormatBool(value)
// // }

// // // Returns sql type for this column.
// // func (Bool) sql() string {
// // 	return "BOOLEAN"
// // }

// // // Returns a pointer to a buffer variable.
// // func (b *Bool) pointer() any {
// // 	return &b.Row
// // }

// // /*
// // Type "Float64", corresponds to the type number in SQL "NUMERIC",
// // this type is child from type "col" and
// // partially implements the interface "column".
// // */

// // type Float64 struct {
// // 	Row float64 // Row for select buffer.
// // 	col         // Base column.
// // }

// // // Creates object type float64 corresponding in sql as "NUMERIC".
// // func (t *Table) Float64(name string, primary bool) *Float64 {
// // 	typ := &Float64{col: col{nam: name, pri: primary, tab: t}}
// // 	t.associate(typ)
// // 	return typ
// // }

// // // Adds a row to the buffer before inserting.
// // func (f *Float64) Insert(value float64) {
// // 	f.tab.insert(f, value)
// // }

// // // Updates the value in a column.
// // func (f *Float64) Update(value float64, where *where) {

// // 	whe := ""
// // 	if where != nil {
// // 		whe = " " + where.sql()
// // 	}
// // 	_, err := f.tab.sch.dat.trx.Exec(fmt.Sprintf("UPDATE %s.%s SET %s = %f%s;", f.tab.sch.nam, f.tab.nam, f.nam, value, whe))
// // 	xlog.Fatalln(err)
// // }

// // // Order by asc.
// // // func (f *Float64) Asc(columns ...column) *order {
// // // 	o := &order{asc: true, tab: f.tab}
// // // 	o.add(f)
// // // 	o.add(columns...)
// // // 	return o
// // // }

// // // Order by desc.
// // // func (f *Float64) Desc(columns ...column) *order {
// // // 	o := &order{asc: false, tab: f.tab}
// // // 	o.add(f)
// // // 	o.add(columns...)
// // // 	return o
// // // }

// // // Returns a pointer to object where with operator "=" as equal.
// // // func (f *Float64) Equal(value float64) *where {
// // // 	return &where{col: f, ope: "=", val: f.format(value)}
// // // }

// // // Returns a pointer to object where with operator "IN".
// // // func (f *Float64) In(values ...float64) *where {

// // // 	str := make([]string, len(values))

// // // 	for vid := 0; vid < len(values); vid++ {
// // // 		str[vid] = f.format(values[vid])
// // // 	}

// // // 	return &where{col: f, ope: "IN", val: "(" + strings.Join(str, ", ") + ")"}
// // // }

// // // Returns a pointer to object where with operator ">" 	greater than.
// // // func (f *Float64) Greater(value float64) *where {
// // // 	return &where{col: f, ope: ">", val: f.format(value)}
// // // }

// // // Returns a pointer to object where with operator "<" less than.
// // // func (f *Float64) Less(value float64) *where {
// // // 	return &where{col: f, ope: "<", val: f.format(value)}
// // // }

// // // Returns a pointer to object where with operator ">=" greater than or equal.
// // // func (f *Float64) GreaterOrEqual(value float64) *where {
// // // 	return &where{col: f, ope: ">=", val: f.format(value)}
// // // }

// // // Returns a pointer to object where with operator "<=" less than or equal.
// // // func (f *Float64) LessOrEqual(value float64) *where {
// // // 	return &where{col: f, ope: "<=", val: f.format(value)}
// // // }

// // // Returns a pointer to object where with operator "<> or !=" not equal.
// // // func (f *Float64) NotEqual(value float64) *where {
// // // 	return &where{col: f, ope: "<> or !=", val: f.format(value)}
// // // }

// // // func (f *Float64) format(value float64) string {
// // // 	return fmt.Sprintf("'%f'", value)
// // // }

// // // Returns sql type for this column.
// // func (Float64) sql() string {
// // 	return "NUMERIC"
// // }

// // // Returns a pointer to a buffer variable.
// // func (f *Float64) pointer() any {
// // 	return &f.Row
// // }

// // // Returns a pointer to object where with operator "=" as equal.
// // func (i *Int64) Equal(value int64) *where {
// // 	return &where{col: i, ope: "=", val: i.format(value)}
// // }

// // // Returns a pointer to object where with operator "IN".
// // func (i *Int64) In(values ...int64) *where {

// // 	str := make([]string, len(values))

// // 	for vid := 0; vid < len(values); vid++ {
// // 		str[vid] = i.format(values[vid])
// // 	}

// // 	return &where{col: i, ope: "IN", val: "(" + strings.Join(str, ", ") + ")"}
// // }

// // // Returns a pointer to object where with operator ">" 	greater than.
// // func (i *Int64) Greater(value int64) *where {
// // 	return &where{col: i, ope: ">", val: i.format(value)}
// // }

// // // Returns a pointer to object where with operator "<" less than.
// // func (i *Int64) Less(value int64) *where {
// // 	return &where{col: i, ope: "<", val: i.format(value)}
// // }

// // // Returns a pointer to object where with operator ">=" greater than or equal.
// // func (i *Int64) GreaterOrEqual(value int64) *where {
// // 	return &where{col: i, ope: ">=", val: i.format(value)}
// // }

// // // Returns a pointer to object where with operator "<=" less than or equal.
// // func (i *Int64) LessOrEqual(value int64) *where {
// // 	return &where{col: i, ope: "<=", val: i.format(value)}
// // }

// // // Returns a pointer to object where with operator "<> or !=" not equal.
// // func (i *Int64) NotEqual(value int64) *where {
// // 	return &where{col: i, ope: "<> or !=", val: i.format(value)}
// // }

// // func (i *Int64) format(value int64) string {
// // 	return fmt.Sprintf("'%d'", value)
// // }

// // // Returns sql type for this column.
// // func (Int64) sql() string {
// // 	return "INT8"
// // }

// // // Returns a pointer to a buffer variable.
// // func (i *Int64) pointer() any {
// // 	return &i.Row
// // }

// // /*
// // Type "String", corresponds to the type number in SQL "TEXT",
// // this type is child from type "col" and
// // partially implements the interface "column".
// // */

// // type String struct {
// // 	Row string // Row for select buffer.
// // 	col        // Base column.
// // }

// // // Creates object type string corresponding in sql as text.
// // func (t *Table) String(name string, primary bool) *String {
// // 	typ := &String{col: col{nam: name, pri: primary, tab: t}}
// // 	t.associate(typ)
// // 	return typ
// // }

// // // Adds a row to the buffer before inserting.
// // func (s *String) Insert(value string) {
// // 	s.tab.insert(s, value)
// // }

// // // Updates the value in a column.
// // func (s *String) Update(value string, where *where) {

// // 	whe := ""
// // 	if where != nil {
// // 		whe = " " + where.sql()
// // 	}
// // 	_, err := s.tab.sch.dat.trx.Exec(fmt.Sprintf("UPDATE %s.%s SET %s = %s%s;", s.tab.sch.nam, s.tab.nam, s.nam, s.format(value), whe))
// // 	xlog.Fatalln(err)
// // }

// // // Order by asc.
// // func (s *String) Asc(columns ...column) *order {
// // 	o := &order{asc: true, tab: s.tab}
// // 	o.add(s)
// // 	o.add(columns...)
// // 	return o
// // }

// // // Order by desc.
// // func (s *String) Desc(columns ...column) *order {
// // 	o := &order{asc: false, tab: s.tab}
// // 	o.add(s)
// // 	o.add(columns...)
// // 	return o
// // }

// // // Returns a pointer to object where with operator "=" as equal.
// // func (s *String) Equal(value string) *where {
// // 	return &where{col: s, ope: "=", val: s.format(value)}
// // }

// // // Returns a pointer to object where with operator "IN".
// // func (s *String) In(values ...string) *where {

// // 	str := make([]string, len(values))

// // 	for i := 0; i < len(values); i++ {
// // 		str[i] = s.format(values[i])
// // 	}

// // 	return &where{col: s, ope: "IN", val: "(" + strings.Join(str, ", ") + ")"}
// // }

// // // Returns a pointer to object where with operator ">" 	greater than.
// // func (s *String) Greater(value string) *where {
// // 	return &where{col: s, ope: ">", val: s.format(value)}
// // }

// // // Returns a pointer to object where with operator "<" less than.
// // func (s *String) Less(value string) *where {
// // 	return &where{col: s, ope: "<", val: s.format(value)}
// // }

// // // Returns a pointer to object where with operator ">=" greater than or equal.
// // func (s *String) GreaterOrEqual(value string) *where {
// // 	return &where{col: s, ope: ">=", val: s.format(value)}
// // }

// // // Returns a pointer to object where with operator "<=" less than or equal.
// // func (s *String) LessOrEqual(value string) *where {
// // 	return &where{col: s, ope: "<=", val: s.format(value)}
// // }

// // // Returns a pointer to object where with operator "<> or !=" not equal.
// // func (s *String) NotEqual(value string) *where {
// // 	return &where{col: s, ope: "<> or !=", val: s.format(value)}
// // }

// // func (s *String) format(value string) string {
// // 	return fmt.Sprintf("'%s'", value)
// // }

// // // Returns sql type for this column.
// // func (String) sql() string {
// // 	return "TEXT"
// // }

// // // Returns a pointer to a buffer variable.
// // func (s *String) pointer() any {
// // 	return &s.Row
// // }

// // /*
// // Type "Time", corresponds to the type number in SQL "TIMESTAMP",
// // this type is child from type "col" and
// // partially implements the interface "column".
// // */

// // type Time struct {
// // 	Row time.Time // Row for select buffer.
// // 	col           // Base column.
// // }

// // // Creates object type time.Time corresponding in sql as timestamp.
// // func (t *Table) Time(name string, primary bool) *Time {
// // 	typ := &Time{col: col{nam: name, pri: primary, tab: t}}
// // 	t.associate(typ)
// // 	return typ
// // }

// // // Adds a row to the buffer before inserting.
// // func (t *Time) Insert(value time.Time) {
// // 	t.tab.insert(t, t.format(value.UTC()))
// // }

// // // Updates the value in a column.
// // func (t *Time) Update(value time.Time, where *where) {

// // 	whe := ""
// // 	if where != nil {
// // 		whe = " " + where.sql()
// // 	}
// // 	_, err := t.tab.sch.dat.trx.Exec(fmt.Sprintf("UPDATE %s.%s SET %s = %s%s;", t.tab.sch.nam, t.tab.nam, t.nam, t.format(value), whe))
// // 	xlog.Fatalln(err)
// // }

// // // Order by asc.
// // func (t *Time) Asc(columns ...column) *order {
// // 	o := &order{asc: true, tab: t.tab}
// // 	o.add(t)
// // 	o.add(columns...)
// // 	return o
// // }

// // // Order by desc.
// // func (t *Time) Desc(columns ...column) *order {
// // 	o := &order{asc: false, tab: t.tab}
// // 	o.add(t)
// // 	o.add(columns...)
// // 	return o
// // }

// // // Returns a pointer to object where with operator "=" as equal.
// // func (t *Time) Equal(value time.Time) *where {
// // 	return &where{col: t, ope: "=", val: t.format(value)}
// // }

// // // Returns a pointer to object where with operator "IN".
// // func (t *Time) In(values ...time.Time) *where {

// // 	str := make([]string, len(values))

// // 	for vid := 0; vid < len(values); vid++ {
// // 		str[vid] = t.format(values[vid])
// // 	}

// // 	return &where{col: t, ope: "IN", val: "(" + strings.Join(str, ", ") + ")"}
// // }

// // // Returns a pointer to object where with operator ">" 	greater than.
// // func (t *Time) Greater(value time.Time) *where {
// // 	return &where{col: t, ope: ">", val: t.format(value)}
// // }

// // // Returns a pointer to object where with operator "<" less than.
// // func (t *Time) Less(value time.Time) *where {
// // 	return &where{col: t, ope: "<", val: t.format(value)}
// // }

// // // Returns a pointer to object where with operator ">=" greater than or equal.
// // func (t *Time) GreaterOrEqual(value time.Time) *where {
// // 	return &where{col: t, ope: ">=", val: t.format(value)}
// // }

// // // Returns a pointer to object where with operator "<=" less than or equal.
// // func (t *Time) LessOrEqual(value time.Time) *where {
// // 	return &where{col: t, ope: "<=", val: t.format(value)}
// // }

// // // Returns a pointer to object where with operator "<> or !=" not equal.
// // func (t *Time) NotEqual(value time.Time) *where {
// // 	return &where{col: t, ope: "<> or !=", val: t.format(value)}
// // }

// // func (t *Time) format(value time.Time) string {
// // 	return fmt.Sprintf("'%d-%02d-%02d %02d:%02d:%02d.%d'", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())
// // }

// // // Returns sql type for this column.
// // func (Time) sql() string {
// // 	return "TIMESTAMP WITHOUT TIME ZONE"
// // }

// // // Returns a pointer to a buffer variable.
// // func (t *Time) pointer() any {
// // 	return &t.Row
// // }
