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

// Map of specifies the sort order by column, true is ASC.
type sort map[column]bool

func (s sort) Asc(by column) {
	s[by] = true
}
func (s sort) Desc(by column) {
	s[by] = false
}

func (s sort) query(table *table) string {

	itm := make([]string, 0)

	sql := ""

	for c, b := range s {

		if b {
			itm = append(itm, fmt.Sprintf("%s.%s ASC", table.from(), c.nam()))
		} else {
			itm = append(itm, fmt.Sprintf("%s.%s DESC", table.from(), c.nam()))
		}
	}

	if len(itm) > 0 {
		sql = fmt.Sprintf("ORDER BY %s", strings.Join(itm, ", "))
	}

	return sql
}
