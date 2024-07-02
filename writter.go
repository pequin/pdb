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
type Writter writter
type writter struct {
	dta *data
}

func (w *Writter) init(data *data) error {

	if data == nil {
		return errors.New("pointer to data is null")
	}

	w.dta = data

	return nil
}

func (d *data) NewWritter() *Writter {

	w := &Writter{}

	if err := w.init(d); err != nil {
		log.Fatalf("Error data in table \"%s\" new writter: %s.", d.tbe.nam, err.Error())
	}

	return w
}

func (w *Writter) Insert(row ...insert) {

	if len(row) != w.dta.tbe.Columns.len() {
		log.Fatalln("Error writter insert: line row does not match header length.")
	}

	ber := make([]any, w.dta.tbe.Columns.len())
	vue := make([]string, w.dta.tbe.Columns.len())

	her, err := w.dta.tbe.Columns.header()
	if err != nil {
		log.Fatalf("Error writter insert: %s.", err.Error())
	}

	for i := 0; i < w.dta.tbe.Columns.len(); i++ {

		idx, err := w.dta.tbe.Columns.index(row[i].cun)
		if err != nil {
			log.Fatalf("Error writter insert: %s.", err.Error())
		}

		ber[idx] = row[i].vue
		vue[idx] = fmt.Sprintf("$%d", idx+1)
	}

	qry := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s);", w.dta.tbe.sma.nam, w.dta.tbe.nam, strings.Join(her, ", "), strings.Join(vue, ", "))

	fmt.Println("qry", qry)

	if err := w.dta.tbe.sma.dbe.begin(); err != nil {
		log.Fatalf("Error writter insert: %s.", err.Error())
	}

	if w.dta.tbe.sma.dbe.ttn == nil {
		log.Fatalln("Error writter insert: pointer to transaction is null.")
	}

	if _, err := w.dta.tbe.sma.dbe.ttn.Exec(qry, ber...); err != nil {
		log.Fatalf("Error writter insert: %s.", err.Error())
	}
}
