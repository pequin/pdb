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

type Reader reader
type reader struct {
	dta    *data
	ber    []any
	her    []string
	lit    uint64
	ost    uint64
	Filter filters
	Sort   sorting
}

func (r *Reader) init(data *data, row ...listener) error {

	if data == nil {
		return errors.New("pointer to data is null")
	}

	lts := make([]*listener, data.tbe.Columns.len())

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
			r.her = append(r.her, fmt.Sprintf("%s.%s.%s", lts[i].cun.table().sma.nam, lts[i].cun.table().nam, lts[i].cun.name()))
		}
	}

	r.dta = data

	return nil
}

func (d *data) NewReader(row ...listener) *Reader {

	r := &Reader{}

	if err := r.init(d, row...); err != nil {
		log.Fatalf("Error data in table \"%s\" new reader: %s.", d.tbe.nam, err.Error())
	}

	return r
}

func (r *Reader) Limit(count uint64) {
	r.lit = count
}
func (r *Reader) Offset(skip uint64) {
	r.ost = skip
}

func (r *Reader) Read(row func()) {

	qry := make([]string, 0)

	// Select.
	qry = append(qry, fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(r.her, ", "), r.dta.tbe.sma.nam, r.dta.tbe.nam))

	// Where.
	if whe, err := r.Filter.where(); err != nil && !errors.Is(errFiltersWhereIsEmpty, err) {
		log.Fatalf("Error reader read: %s.", err.Error())
	} else if !errors.Is(errFiltersWhereIsEmpty, err) {
		qry = append(qry, whe)
	}

	// Order.
	if odr, err := r.Sort.order(); err != nil && !errors.Is(errSortingOrderIsEmpty, err) {
		log.Fatalf("Error reader read: %s.", err.Error())
	} else if !errors.Is(errSortingOrderIsEmpty, err) {
		qry = append(qry, odr)
	}

	// Limit.
	if r.lit > 0 {
		qry = append(qry, fmt.Sprintf("LIMIT %d", r.lit))
	}
	//  Offset.
	if r.ost > 0 {
		qry = append(qry, fmt.Sprintf("OFFSET %d", r.ost))
	}

	rws, err := r.dta.tbe.sma.dbe.pgd.Query(strings.Join(qry, " "))
	if err != nil {
		log.Fatalf("Error reader read: %s.", err.Error())
	}

	for rws.Next() {

		if err := rws.Scan(r.ber...); err == nil {
			row()
		} else {
			log.Fatalf("Error reader read: %s.", err.Error())
		}
	}
}
