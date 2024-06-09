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
	Sort   sort           // Specifies the sort order.
	Offset offset         // Skip that many rows before beginning to return rows.
	Limit  limit          // Limit count is given, no more than that many rows will be returned (but possibly fewer, if the query itself yields fewer rows).
	thd    *thread        // Thread.
	tab    *table         // Table.
	flt    *filter        // Filter.
	rid    map[column]int // Rows.
	buf    []any          // Buffer.
}

// Returms new reader for context.
func (t *thread) Reader(table *table) *reader {

	rad := &reader{Sort: make(sort), Offset: 0, Limit: 0, thd: t, tab: table, rid: make(map[column]int), buf: make([]any, len(table.col))}

	for i := 0; i < len(table.col); i++ {
		rad.rid[table.col[i]] = i
		rad.buf[i] = table.col[i].buf()
	}
	t.begin()

	return rad
}

func (r *reader) Filter(filter *filter) {
	r.flt = filter
}
func (r *reader) query() string {

	arg := make([]string, 0)

	if r.flt != nil {
		arg = append(arg, r.flt.query(r.tab))
	}

	arg = append(arg, r.Sort.query(r.tab))
	arg = append(arg, r.Limit.query())
	arg = append(arg, r.Offset.query())

	sql := fmt.Sprintf("SELECT %s FROM %s", r.tab.columns(), r.tab.from())

	if len(arg) > 0 {
		sql += " " + strings.TrimSpace(strings.Join(arg, " "))
	}

	sql += ";"

	return sql
}

func (r *reader) Read(row func()) {

	rws, err := r.thd.trx.Query(r.query())
	xlog.Fatalln(err)

	defer rws.Close()

	for rws.Next() {
		xlog.Fatalln(rws.Scan(r.buf...))

		if row != nil {
			row()
		}
	}
}
