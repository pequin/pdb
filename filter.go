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
	idx *index   // Main expression.
	nxt []*index // Subsequent expressions.
	log []bool   // Logic for join the next expression - true is as AND, false is as OR.
}

func (f *filter) And(index *index) *filter {
	f.nxt = append(f.nxt, index)
	f.log = append(f.log, true)
	return f
}
func (f *filter) Or(index *index) *filter {
	f.nxt = append(f.nxt, index)
	f.log = append(f.log, false)
	return f
}

func (f *filter) where(table *table) string {

	if f == nil {
		return ""
	}

	str := []string{fmt.Sprintf("%s.%s %s '%s'", table.name(), f.idx.nme, f.idx.opr, f.idx.val)}

	log := ""
	for i := 0; i < len(f.nxt); i++ {

		if f.log[i] {
			log = "AND"
		} else {
			log = "OR"
		}
		str = append(str, fmt.Sprintf("%s %s.%s %s '%s'", log, table.name(), f.nxt[i].nme, f.nxt[i].opr, f.nxt[i].val))
	}

	return "WHERE " + strings.Join(str, " ")
}
