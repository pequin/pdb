package pdb

import (
	"fmt"
	"strings"
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

// type filter map[*where]string

// type fgn struct {
// 	flt map[*where]string // Filters.

// 	bd struct {
// 	}
// }
// type ggggg struct {
// 	bdfb map[*where]struct {
// 		opr string // Operator.
// 		val string
// 	}
// }

// type filter map[*where]string
// type filters map[*where]string

// type filter map[*where]bool

// type filter struct {
// 	whr *where  // Where.
// 	nxt *filter // Next filter.
// 	log bool    // Logical operator - true is as AND, false is as OR.
// }

// type filters struct {
// }

// func (f *filters) Where(filter *filter) *filter {
// 	f = f
// 	return &f
// }

// func (f *filter) And(filter *filter) *filter {
// 	if f == filter {
// 		fmt.Println("trrrr")
// 	}
// 	// e := f.end()
// 	// e.nxt = filter
// 	// e.log = true
// 	return f
// }
// func (f *filter) Or(filter *filter) *filter {
// 	if f == filter {
// 		fmt.Println("trrrr")
// 	}
// 	// e := f.end()
// 	// e.nxt = filter
// 	// e.log = false
// 	return f
// }

// func (f *filter) query(table *table) string {

// 	// p := f
// 	// for p.nxt != nil {

// 	// 	log := "OR"
// 	// 	if p.log {
// 	// 		log = "AND"
// 	// 	}

// 	// 	fmt.Println("FFFF", log)

// 	// 	// itm = append(itm, fmt.Sprintf("%s %s", log, fmt.Sprintf("%s.%s %s %s", table.from(), w.col.nam(), w.opr, w.val)))
// 	// 	p = p.nxt
// 	// }

// 	fmt.Println("gfn", f.nxt)
// 	return "df"
// }

// func (f *filter) end() *filter {

// 	// fmt.Println("FFFF", f.nxt)

// 	p := f
// 	for p.nxt != nil {
// 		fmt.Println("PP", p.nxt)
// 		p = p.nxt
// 	}

//		return p
//	}

// type filters struct {
// }

type filter struct {
	whr *where  // where
	nxt *filter // Next filter.
	log bool    // Logical operator - true is as AND, false is as OR.
}

func (f *filter) Where(where *where) *filter {

	f.whr = where

	return f.whr.Filter()

}

func (f *filter) And(filter *filter) *filter {
	end := f.end()
	end.log = true
	end.nxt = filter
	return f
}
func (f *filter) Or(filter *filter) *filter {
	end := f.end()
	end.log = false
	end.nxt = filter
	return f
}

func (f *filter) query(table *table) string {

	if f == nil {
		return ""
	}

	lst := f.list()

	if len(lst) == 1 {
		return lst[0].whr.string(table)
	}

	sql := make([]string, len(lst))

	for i := 0; i < len(lst); i++ {

		log := "OR"
		if lst[i].log {
			log = "AND"
		}

		if i == len(lst)-1 {
			log = ""
		}

		sql[i] = fmt.Sprintf("(%s) %s", lst[i].whr.string(table), log)

	}

	return strings.TrimSpace("WHERE " + strings.Join(sql, " "))
}

func (f *filter) end() *filter {

	for f.nxt != nil {
		f = f.nxt
	}

	return f
}

func (f *filter) list() []*filter {

	lst := make([]*filter, 0)
	lst = append(lst, f)

	for f.nxt != nil {
		lst = append(lst, f.nxt)
		f = f.nxt
	}

	return lst
}

// type filter struct {
// 	// idx map[*where]int // Indices.
// 	// grp []*grup // Grups.
// 	// idx map[column]int // Indices.
// 	// whr []*where
// 	whr []*where
// 	// col []column       // Columns.
// 	// opr []string       // Operators.
// 	// val []string       // Value.
// 	// log []bool         // Logical operators - true is as AND, false is as OR.

// 	// log bool // Logical operator - true is as AND, false is as OR.
// }

// type otherrr struct {
// }

// func (otherrr) And(where where) *filter {
// 	return nil
// }
// func (otherrr) Or(where where) *filter {
// 	return nil
// }

// func (f *filter) Less(where where) *otherrr {
// 	// w.opr = "<"
// 	// return w
// 	return nil
// }
// func (f *filter) LessOrEqual(where *where) *otherrr {
// 	// w.opr = "<="
// 	// return w
// 	return nil
// }
// func (f *filter) Equal(where where) *otherrr {

// 	// w.opr = "="
// 	// return w
// 	return nil
// }
// func (f *filter) NotEqual(where *where) *otherrr {
// 	// w.opr = "<>"
// 	// return w
// 	return nil
// }
// func (f *filter) Greater(where *where) *otherrr {
// 	// w.opr = ">"
// 	// return w
// 	return nil
// }
// func (w *filter) GreaterOrEqual(where *where) *otherrr {
// 	// w.opr = ">="
// 	// return w

// 	return nil
// }

// func (f *filter) And(filter *filter) *filter {
// 	e := f.end()
// 	e.nxt = filter
// 	e.log = true
// 	return f
// }
// func (f *filter) Or(filter *filter) *filter {
// 	e := f.end()
// 	e.nxt = filter
// 	e.log = false
// 	return f
// }

// func (f *filter) end() *filter {

// 	p := f
// 	for p.nxt != nil {
// 		p = p.nxt
// 	}

// 	return p
// }

/////////////////////////////////////////////////////////////////

// type grup struct {
// 	whr []*where
// 	log bool // Logical operator - true is as AND, false is as OR.
// }

type where struct {
	col column // Column.
	opr string // Operator.
	val string // Value.
	nxt *where // Next where.
	log bool   // Logical operator - true is as AND, false is as OR.
}

func (w *where) And(where *where) *where {
	end := w.end()
	end.log = true
	end.nxt = where
	return w
}
func (w *where) Or(where *where) *where {
	end := w.end()
	end.log = false
	end.nxt = where
	return w
}

func (w *where) Filter() *filter {
	return &filter{whr: w}
}

// query
func (w *where) string(table *table) string {

	if w == nil {
		return ""
	}

	lst := w.list()
	sql := make([]string, len(lst))

	for i := 0; i < len(lst); i++ {

		log := "OR"
		if lst[i].log {
			log = "AND"
		}

		if i == len(lst)-1 {
			log = ""
		}

		sql[i] = fmt.Sprintf("%s.%s %s '%s' %s", table.from(), lst[i].col.nam(), lst[i].opr, lst[i].val, log)

	}

	return strings.TrimSpace(strings.Join(sql, " "))
}

func (w *where) end() *where {

	for w.nxt != nil {
		w = w.nxt
	}

	return w
}

func (w *where) list() []*where {

	lst := make([]*where, 0)
	lst = append(lst, w)

	for w.nxt != nil {
		lst = append(lst, w.nxt)
		w = w.nxt
	}

	return lst
}

// type filter interface {
// 	// idx map[column]int // Indices.
// 	// col []column       // Columns.
// 	// opr []string       // Operators.
// 	// val []*string      // Value.
// 	// log []bool         // Logical operators - true is as AND, false is as OR.
// 	// col column // Column.
// 	// opr string // Operator.
// 	// val string // Value.
// 	// log bool   // Logical operator - true is as AND, false is as OR.
// }

// type where struct {
// 	// idx map[column]int // Indices.
// 	// col []column       // Columns.
// 	// opr []string       // Operators.
// 	// val []*string      // Value.
// 	// log []bool         // Logical operators - true is as AND, false is as OR.
// 	col column // Column.
// 	// opr string // Operator.
// 	val string // Value.
// 	// log bool   // Logical operator - true is as AND, false is as OR.
// }

// func (w *where) Value(col column, val string) {
// 	// *w.val[w.idx[col]] = val
// }

// func (w *where) Less() {
// 	// w.opr = "<"
// 	// return w
// }
// func (w *where) LessOrEqual() {
// 	// w.opr = "<="
// 	// return w
// }
// func (w *where) Equal() {

// 	// w.opr = "="
// 	// return w
// }
// func (w *where) NotEqual() {
// 	// w.opr = "<>"
// 	// return w
// }
// func (w *where) Greater() {
// 	// w.opr = ">"
// 	// return w
// }
// func (w *where) GreaterOrEqual() {
// 	// w.opr = ">="
// 	// return w
// }

// func (w *where) Less() *where {
// 	w.opr = "<"
// 	return w
// }
// func (w *where) LessOrEqual() *where {
// 	w.opr = "<="
// 	return w
// }
// func (w *where) Equal() *where {
// 	w.opr = "="
// 	return w
// }
// func (w *where) NotEqual() *where {
// 	w.opr = "<>"
// 	return w
// }
// func (w *where) Greater() *where {
// 	w.opr = ">"
// 	return w
// }
// func (w *where) GreaterOrEqual() *where {
// 	w.opr = ">="
// 	return w
// }

// func (w *where) query(table *table) string {

// 	if w == nil {
// 		return ""
// 	}

// 	itm := make([]string, 0)
// 	itm = append(itm, fmt.Sprintf("%s.%s %s %s", table.from(), w.col.nam(), w.opr, w.val))

// 	p := w
// 	for p.nxt != nil {

// 		log := "OR"
// 		if p.log {
// 			log = "AND"
// 		}

// 		itm = append(itm, fmt.Sprintf("%s %s", log, fmt.Sprintf("%s.%s %s %s", table.from(), w.col.nam(), w.opr, w.val)))
// 		p = p.nxt
// 	}

// 	return strings.Join(itm, " ")
// }

// opr string // Operator.

// type operator string

// const (
// 	LessThan       operator = "<"
// 	LessOrEqual    operator = "<="
// 	Equal          operator = "="
// 	NotEqual       operator = "<>"
// 	GreaterThan    operator = ">"
// 	GreaterOrEqual operator = ">="
// )

// type where struct {
// 	cu string // Clause.
// 	vl string // Value.
// 	lo string // Logical operator.
// 	nw *where // Next where.
// }

// func (w *where) And(filter *where) {
// 	w.lo = "AND"
// 	w.nw = filter
// }
// func (w *where) Or(filter *where) {
// 	w.lo = "OR"
// 	w.nw = filter
// }
// func (w *where) string(nt string) string {

// 	ft := make([]string, 0) // Filter.
// 	wh := w                 // Next where.

// 	for {

// 		ft = append(ft, nt+"."+wh.cu+" "+wh.vl+" "+wh.lo)

// 		if wh.nw != nil {
// 			wh = w.nw
// 		} else {
// 			break
// 		}
// 	}

// 	return strings.Join(ft, " ")
// }

// type where struct {
// 	col column // Column.
// 	ope string // Operator.
// 	val string // Value.
// 	log string // Logical operator.
// 	nex *where // Next clause.
// }

// func (w *where) And(where *where) {
// 	w.logical(where, "AND")

// }
// func (w *where) Or(where *where) {
// 	w.logical(where, "OR")
// }

// func (w *where) logical(where *where, operator string) {

// 	if w.col.table() != where.col.table() {
// 		xlog.Fatalln("Cannot add columns from another table.")
// 	}

// 	// Prev clause.
// 	p := w
// 	for p.nex != nil {
// 		p = p.nex
// 	}
// 	p.log = operator
// 	p.nex = where
// }

// func (w *where) sql() string {

// 	str := make([]string, 1)

// 	str[0] = fmt.Sprintf("%s %s %s", w.col.name(), w.ope, w.val)

// 	// Prev clause.
// 	p := w
// 	for p.nex != nil {

// 		str = append(str, fmt.Sprintf("%s %s %s %s", p.log, p.nex.col.name(), p.nex.ope, p.nex.val))
// 		p = p.nex
// 	}

// 	return "WHERE " + strings.Join(str, "")
// }
