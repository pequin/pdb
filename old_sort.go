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

type sort struct {
	col []Type // Column.
	asc []bool // Logic for order by - true is as asc, false is as desc.
}

func (s *sort) Asc(by Type) *sort {
	s.col = append(s.col, by)
	s.asc = append(s.asc, true)
	return s

}
func (s *sort) Desc(by Type) *sort {
	s.col = append(s.col, by)
	s.asc = append(s.asc, false)
	return s
}

func (s *sort) order(table *old_table) string {

	if len(s.col) < 1 {
		return ""
	}

	log := ""

	str := make([]string, len(s.col))

	for i := 0; i < len(s.col); i++ {

		if s.asc[i] {
			log = "ASC"
		} else {
			log = "DESC"
		}

		str[i] = fmt.Sprintf("%s.%s %s", table.name(), s.col[i].name(), log)

	}

	return "ORDER BY " + strings.Join(str, ", ")
}
