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
	flt *Filter
	wrs []*Where
	lgs map[*Where]bool
}

func (f *Filter) init() error {
	f.lgs = make(map[*Where]bool)
	f.wrs = make([]*Where, 0)
	return nil
}

func (f *Filter) add(where *Where, log bool) error {

	if where == nil {
		return errors.New("pointer to where is null")
	}

	for i := 0; i < len(f.wrs); i++ {

		if f.wrs[i] == where {
			return fmt.Errorf("filter from column with name \"%s\" is already exist", f.wrs[i].clm.name())
		}
	}

	if is := f.lgs[where]; is {
		return fmt.Errorf("filter from column with name \"%s\" is already exist", where.clm.name())
	}

	f.wrs = append(f.wrs, where)
	f.lgs[where] = log

	return nil
}

func (f *filter) where() (string, error) {

	s := make([]string, len(f.flt.wrs))

	log := "OR"

	for i := 0; i < len(f.flt.wrs); i++ {

		w := f.flt.wrs[i]

		if i == 0 {
			s[i] = fmt.Sprintf("%s.%s.%s %s '%s'", w.clm.table().sma.nme, w.clm.table().nme, w.clm.name(), w.opr, w.vle)
		} else {

			if lg, is := f.flt.lgs[w]; is {
				if lg {
					log = "AND"
				} else {
					log = "OR"
				}
			} else {
				return "", errors.New("missing logical operator")
			}

			s[i] = fmt.Sprintf("%s %s.%s.%s %s '%s'", log, w.clm.table().sma.nme, w.clm.table().nme, w.clm.name(), w.opr, w.vle)
		}
	}

	return fmt.Sprintf("WHERE %s", strings.Join(s, " ")), nil
}

func (f *filter) By(where *Where) *Filter {

	if where == nil {
		log.Fatalln("Error filter by: pointer to where is null.")
	}
	flt := &Filter{}

	if err := flt.init(); err != nil {
		log.Fatalf("Error filter by: %s.", err.Error())
	}

	flt.wrs = append(flt.wrs, where)
	f.flt = flt

	return flt
}

func (f *Filter) And(where *Where) *Filter {

	if err := f.add(where, true); err != nil {
		log.Fatalf("Error filter and: %s.", err.Error())
	}
	return f
}

func (f *Filter) Or(where *Where) *Filter {

	if err := f.add(where, false); err != nil {
		log.Fatalf("Error filter or: %s.", err.Error())
	}
	return f
}
