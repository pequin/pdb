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

type filter struct {
	whr *where  // where
	nxt *filter // Next filter.
	log bool    // Logical operator - true is as AND, false is as OR.
}

type where struct {
	col column // Column.
	opr string // Operator.
	val string // Value.
	nxt *where // Next where.
	log bool   // Logical operator - true is as AND, false is as OR.
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
		return "WHERE " + lst[0].whr.string(table)
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
