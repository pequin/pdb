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

type order struct {
	col column // Column.
	asc bool   // Logic for order by - true is as asc, false is as desc.
}
type sort struct {
	ord []order
}

func (s *sort) Asc(by column) {
	s.ord = append(s.ord, order{col: by, asc: true})

}
func (s *sort) Desc(by column) {
	s.ord = append(s.ord, order{col: by, asc: false})
}

func (s *sort) string(table *table) string {

	if s == nil {
		return ""
	}

	str := make([]string, 0)

	log := ""

	for i := 0; i < len(s.ord); i++ {

		if s.ord[i].asc {
			log = "ASC"
		} else {
			log = "DESC"
		}

		str = append(str, fmt.Sprintf("%s.%s %s", table.name(), s.ord[i].col.nam(), log))
	}

	return "ORDER BY " + strings.Join(str, ", ")
}
