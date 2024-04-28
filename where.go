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

type where struct {
	col column // Column.
	ope string // Operator.
	val string // Value.
	log string // Logical operator.
	nex *where // Next clause.
}

func (w *where) And(where *where) {
	w.logical(where, "AND")

}
func (w *where) Or(where *where) {
	w.logical(where, "OR")
}

func (w *where) logical(where *where, operator string) {

	if w.col.table() != where.col.table() {
		xlog.Fatalln("Cannot add columns from another table.")
	}

	// Prev clause.
	p := w
	for p.nex != nil {
		p = p.nex
	}
	p.log = operator
	p.nex = where
}

func (w *where) sql() string {

	str := make([]string, 1)

	str[0] = fmt.Sprintf("%s %s %s", w.col.name(), w.ope, w.val)

	// Prev clause.
	p := w
	for p.nex != nil {

		str = append(str, fmt.Sprintf("%s %s %s %s", p.log, p.nex.col.name(), p.nex.ope, p.nex.val))
		p = p.nex
	}

	return "WHERE " + strings.Join(str, "")
}
