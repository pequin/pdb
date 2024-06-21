package pdb

import (
	"fmt"
	"strings"

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

type writer struct {
	tbl *table // Table.
	qry string //Query
}

func (t *table) Writer() *writer {

	t.init()

	l := len(t.Column.hdr)

	h := make([]string, l) // Headers.
	v := make([]string, l) // Variables.

	for i := 0; i < l; i++ {
		h[i] = t.Column.hdr[i].nam
		v[i] = fmt.Sprintf("$%d", i+1)
	}

	return &writer{tbl: t, qry: fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", t.name(), strings.Join(h, ", "), strings.Join(v, ", "))}
}

func (w *writer) Row(values ...into) {

	msd := make([]string, 0)                  // Missed columns.
	buf := make([]any, len(w.tbl.Column.hdr)) // Buffer.
	isp := true                               // Is present.

	for hi := 0; hi < len(w.tbl.Column.hdr); hi++ {

		isp = true

		for ri := 0; ri < len(values); ri++ {

			if isp && w.tbl.Column.hdr[hi] == values[ri].hdr {
				buf[hi] = values[ri].val
				isp = false
			}
		}

		if isp {
			msd = append(msd, w.tbl.Column.hdr[hi].nam)
		}
	}

	if len(msd) == 1 {
		xlog.Fatallf("Row - missing column: %s", strings.Join(msd, ", "))
	} else if len(msd) > 1 {
		xlog.Fatallf("Row - missing columns: %s", strings.Join(msd, ", "))
	}

	_, err := w.tbl.stx.Exec(w.qry, buf...)
	xlog.Fatalln(err)

}
