package pdb

import (
	"errors"
	"fmt"
	"log"
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

type Reader reader
type reader struct {
	dta *data
	// lts []Listener
	buf    []any
	Filter filter
}

func (r *Reader) init(data *data, row ...Listener) error {

	if data == nil {
		return errors.New("pointer to data is null")
	}

	lts := make([]*Listener, data.tbl.Columns.len())

	if len(row) < 1 {
		return errors.New("no listeners")
	}

	for ri := 0; ri < len(row); ri++ {

		if idx, err := data.tbl.Columns.index(row[ri].clm); err == nil {

			// Checking duplication.
			for i := 0; i < data.tbl.Columns.len(); i++ {

				if lts[i] != nil {

					if lts[i].clm == row[ri].clm {
						return fmt.Errorf("listener in column with name \"%s\" already exists", lts[i].clm.name())
					}
					if lts[i].buf == row[ri].buf {
						return fmt.Errorf("at a column with name \"%s\" listener pointer matches the listener in column with name \"%s\" which was added earlier", row[ri].clm.name(), lts[i].clm.name())
					}
				}
			}

			lts[idx] = &row[ri]

		} else {
			return err
		}
	}

	for i := 0; i < data.tbl.Columns.len(); i++ {

		if lts[i] != nil {
			r.buf = append(r.buf, lts[i].buf)
		}
	}

	r.dta = data

	return nil
}

func (d *data) NewReader(row ...Listener) *Reader {

	r := &Reader{}

	if err := r.init(d, row...); err != nil {
		log.Fatalf("Error data in table \"%s\" new reader: %s.", d.tbl.nme, err.Error())
	}

	// for i := 0; i < len(r.buf); i++ {

	// 	fdbdf := *r.buf[i].(*int64)

	// 	fmt.Println("rr", fdbdf)

	// }

	return r
}
