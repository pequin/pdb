package pdb

import (
	"errors"
	"fmt"
	"log"
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

type Filter filter
type filter struct {
	flt *filters
}
type filters struct {
	whe []*Where        // Where.
	lis map[*Where]bool // Logics.
}

func (f *filters) init() {
	f.whe = make([]*Where, 0)
	f.lis = make(map[*Where]bool)
}

func (f *Filter) init(filters *filters) error {
	if filters == nil {
		return errors.New("pointer to data is null")
	}
	f.flt = filters

	return nil
}

func (f *filters) add(where *Where, log bool) error {

	if where == nil {
		return errors.New("pointer to where is null")
	}

	for i := 0; i < len(f.whe); i++ {

		if f.whe[i] == where {
			return fmt.Errorf("filter from column with name \"%s\" is already exist", f.whe[i].cln.name())
		}
	}

	if is := f.lis[where]; is {
		return fmt.Errorf("filter from column with name \"%s\" is already exist", where.cln.name())
	}

	f.whe = append(f.whe, where)
	f.lis[where] = log

	return nil
}

var errFiltersWhereIsEmpty = errors.New("where is empty")

func (f *filters) where() (string, error) {

	if len(f.whe) < 1 {
		return "", errFiltersWhereIsEmpty
	}

	s := make([]string, len(f.whe))

	for i := 0; i < len(f.whe); i++ {

		w := f.whe[i]

		if i == 0 {
			s[i] = fmt.Sprintf("%s.%s.%s %s '%s'", w.cln.table().sma.nam, w.cln.table().nam, w.cln.name(), w.cmn, w.vle)
		} else {

			if l, b := f.lis[w]; b {
				s[i] = fmt.Sprintf("%s %s.%s.%s %s '%s'", strings.NewReplacer("true", "AND", "false", "OR").Replace(fmt.Sprintf("%t", l)), w.cln.table().sma.nam, w.cln.table().nam, w.cln.name(), w.cmn, w.vle)
			} else {
				return "", errors.New("missing logical operator")
			}

		}
	}

	return fmt.Sprintf("WHERE %s", strings.Join(s, " ")), nil
}

func (f *filters) By(where *Where) *Filter {

	f.init()

	if where == nil {
		log.Fatalln("Error filters by: pointer to where is null.")
	}
	flt := &Filter{}

	if err := flt.init(f); err != nil {
		log.Fatalf("Error filters by: %s.", err.Error())
	}

	f.whe = append(f.whe, where)

	return flt
}

func (f *Filter) And(where *Where) *Filter {

	if err := f.flt.add(where, true); err != nil {
		log.Fatalf("Error filter and: %s.", err.Error())
	}
	return f
}

func (f *Filter) Or(where *Where) *Filter {

	if err := f.flt.add(where, false); err != nil {
		log.Fatalf("Error filter or: %s.", err.Error())
	}
	return f
}
