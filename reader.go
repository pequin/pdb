package pdb

import (
	"fmt"
	"strconv"
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
	Sort sort    // Specifies the sort order.
	tbl  *table  // Table.
	flt  *filter // Filter.
	cls  string  //Columns
	lim  uint64  // Limit count is given, no more than that many rows will be returned (but possibly fewer, if the query itself yields fewer rows).
	off  uint64  // Skip that many rows before beginning to return rows.
	buf  []any   // Buffer.
}

func (r *reader) Filter(where *filter) {
	r.flt = where
}

func (r *reader) Limit(value uint64) {
	r.lim = value
}

func (r *reader) Offset(value uint64) {
	r.off = value
}

func (r *reader) limit() string {

	str := "NULL"
	if r.lim > 0 {
		str = strconv.FormatUint(uint64(r.lim), 10)
	}

	return fmt.Sprintf("LIMIT %s", str)
}
func (r *reader) offset() string {

	str := "NULL"
	if r.off > 0 {
		str = strconv.FormatUint(uint64(r.off), 10)
	}

	return fmt.Sprintf("OFFSET %s", str)
}

func (r *reader) query(table *table) string {

	table.init()

	str := []string{fmt.Sprintf("SELECT %s FROM %s", r.cls, table.name())}

	if w := r.flt.where(table); len(w) > 0 {
		str = append(str, w)
	}

	if s := r.Sort.order(table); len(s) > 0 {
		str = append(str, s)
	}

	str = append(str, r.limit())
	str = append(str, r.offset())

	return strings.Join(str, " ")
}

func (r *reader) Rows(row func()) {

	qry := r.query(r.tbl)

	rws, err := r.tbl.stx.Query(qry)
	xlog.Fatalln(err)

	for rws.Next() {

		xlog.Fatalln(rws.Scan(r.buf...))

		row()

	}
}
