package pdb

import (
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

type order struct {
	tab *Table   // Table.
	asc bool     // Sort alphabetically.
	col []string // Names of columns.
}

func (o *order) add(columns ...column) {

	for i := 0; i < len(columns); i++ {
		if o.tab.isAssociated(columns[i]) {
			o.col = append(o.col, columns[i].name())
		} else {
			xlog.Fatallf("The column \"%s\" is not associated with the table: %s.", columns[i].name(), columns[i].table().nam)
		}
	}
}

func (o *order) sql() string {

	asc := " ASC"
	if !o.asc {
		asc = " DESC"
	}

	return "ORDER BY " + strings.Join(o.col, ", ") + asc
}
