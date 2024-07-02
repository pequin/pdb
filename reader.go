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
	dta    *data
	ber    []any
	Filter filters
	Sort   sorting
}

func (r *Reader) init(data *data, row ...Listener) error {

	if data == nil {
		return errors.New("pointer to data is null")
	}

	lts := make([]*Listener, data.tbe.Columns.len())

	if len(row) < 1 {
		return errors.New("no listeners")
	}

	for ri := 0; ri < len(row); ri++ {

		if idx, err := data.tbe.Columns.index(row[ri].cun); err == nil {

			// Checking duplication.
			for i := 0; i < data.tbe.Columns.len(); i++ {

				if lts[i] != nil {

					if lts[i].cun == row[ri].cun {
						return fmt.Errorf("listener in column with name \"%s\" already exists", lts[i].cun.name())
					}
					if lts[i].ber == row[ri].ber {
						return fmt.Errorf("at a column with name \"%s\" listener pointer matches the listener in column with name \"%s\" which was added earlier", row[ri].cun.name(), lts[i].cun.name())
					}
				}
			}

			lts[idx] = &row[ri]

		} else {
			return err
		}
	}

	for i := 0; i < data.tbe.Columns.len(); i++ {

		if lts[i] != nil {
			r.ber = append(r.ber, lts[i].ber)
		}
	}

	r.dta = data

	return nil
}

func (d *data) NewReader(row ...Listener) *Reader {

	r := &Reader{}

	if err := r.init(d, row...); err != nil {
		log.Fatalf("Error data in table \"%s\" new reader: %s.", d.tbe.nam, err.Error())
	}

	// for i := 0; i < len(r.buf); i++ {

	// 	fdbdf := *r.buf[i].(*int64)

	// 	fmt.Println("rr", fdbdf)

	// }

	return r
}

func (r *Reader) Ruuunn() {

	qqq := make([]string, 0)

	// Where.
	if whe, err := r.Filter.where(); err != nil && !errors.Is(errWhereIsEmpty, err) {
		log.Fatalf("Error reader ruuunn: %s.", err.Error())
	} else if !errors.Is(errWhereIsEmpty, err) {
		qqq = append(qqq, whe)
	}
	// Order.
	if odr, err := r.Sort.order(); err != nil && !errors.Is(errOrderIsEmpty, err) {
		log.Fatalf("Error reader ruuunn: %s.", err.Error())
	} else if !errors.Is(errOrderIsEmpty, err) {
		qqq = append(qqq, odr)
	}

	fmt.Println(qqq)

}
