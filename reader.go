package pdb

import (
	"fmt"
	"strings"

	"github.com/pequin/xlog"
)

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

type reader struct {
	tbl *table         // Table.
	col []column       // Columns.
	flt *filter        // Filter.
	srt *sort          // Specifies the sort order.
	off offset         // Skip that many rows before beginning to return rows.
	lim limit          // Limit count is given, no more than that many rows will be returned (but possibly fewer, if the query itself yields fewer rows).
	idx map[column]int // Column indexes.
	buf []any          // Buffer.
}

// Makes new reader and returns pointer to it.
func (t *table) Reader(columns ...column) *reader {

	if len(columns) < 1 {
		xlog.Fatalln("When creating new reader, you must specify at least one column.")
	}

	r := &reader{tbl: t, col: columns, off: 0, lim: 0, srt: nil, flt: nil, idx: make(map[column]int), buf: make([]any, len(columns))}

	for i := 0; i < len(r.col); i++ {
		r.idx[r.col[i]] = i
		r.buf[i] = r.col[i].buf()
	}

	return r
}

func (r *reader) Filter(where *where) *filter {

	r.flt = &filter{whr: where}

	return r.flt
}

func (r *reader) Sort() *sort {
	if r.srt == nil {
		r.srt = &sort{ord: make([]order, 0)}
	}
	return r.srt
}

func (r *reader) Limit(value uint64) {
	r.lim = limit(value)
}

func (r *reader) Offset(value uint64) {
	r.off = offset(value)
}

func (r *reader) from(table *table) string {

	str := make([]string, len(r.col))

	for i := 0; i < len(r.col); i++ {
		str[i] = fmt.Sprintf("%s.%s", table.name(), r.col[i].nam())
	}

	return "SELECT " + strings.Join(str, ", ") + " FROM " + table.name()
}

func (r *reader) string() string {

	frm := r.from(r.tbl)
	flt := r.flt.string(r.tbl)
	srt := r.srt.string(r.tbl)
	lim := r.lim.string()
	off := r.off.string()

	return strings.Join([]string{frm, flt, srt, lim, off}, " ") + ";"
}

func (r *reader) Read(row func()) {

	if row != nil && r.tbl.stx != nil {

		rws, err := r.tbl.stx.Query(r.string())
		xlog.Fatalln(err)
		defer rws.Close()

		for rws.Next() {
			xlog.Fatalln(rws.Scan(r.buf...))
			row()
		}
	}
}
