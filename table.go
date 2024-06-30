package pdb

import (
	"context"
	"database/sql"
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

type Table table
type table struct {
	nme     string
	sma     *Schema
	stx     *sql.Tx
	Columns columns
}

func (t *Table) init(name string, schema *Schema) error {

	name = strings.TrimSpace(name)

	if len(name) < 1 {
		return errors.New("name is not specified")
	}

	if schema == nil {
		return errors.New("pointer to schema is null")
	}

	t.nme = name
	t.sma = schema

	if err := t.Columns.init(t); err != nil {
		return err
	}

	return nil
}

func (t *Table) begin() error {

	if t.stx == nil {

		stx, err := t.sma.dba.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
		if err != nil {
			return err
		}

		t.stx = stx
	}

	return nil
}

func (t *Table) commit() error {
	if t.stx == nil {
		return fmt.Errorf("pointer to transaction is null")
	}

	if err := t.stx.Commit(); err != nil {

		if err := t.stx.Rollback(); err != nil {
			return err
		}

		return err
	}

	t.stx = nil

	return nil

}

func (t *Table) Create() {

	dts, err := t.Columns.datatypes()

	if err != nil {
		log.Fatalf("Error table create: %s.", err.Error())
	}

	if _, err := t.sma.dba.db.Exec(fmt.Sprintf("CREATE TABLE %s.%s (%s)", t.sma.nme, t.nme, strings.Join(dts, ", "))); err != nil {
		log.Fatalf("Error table create: %s.", err.Error())
	}
}

func (t *Table) Insert(row ...insert) {

	if len(row) != t.Columns.len() {
		log.Fatalln("Error table insert: line row does not match header length.")
	}

	buf := make([]any, t.Columns.len())
	vls := make([]string, t.Columns.len())

	hdr, err := t.Columns.header()
	if err != nil {
		log.Fatalf("Error table insert: %s.", err.Error())
	}

	for i := 0; i < t.Columns.len(); i++ {

		idx, err := t.Columns.index(row[i].clm)
		if err != nil {
			log.Fatalf("Error table insert: %s.", err.Error())
		}

		buf[idx] = row[i].vle
		vls[idx] = fmt.Sprintf("$%d", idx+1)
	}

	qry := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s);", t.sma.nme, t.nme, strings.Join(hdr, ", "), strings.Join(vls, ", "))

	if err := t.begin(); err != nil {
		log.Fatalf("Error table insert: %s.", err.Error())
	}

	if t.stx == nil {
		log.Fatalln("Error table insert: pointer to transaction is null.")
	}

	if _, err := t.stx.Exec(qry, buf...); err != nil {
		log.Fatalf("Error table insert: %s.", err.Error())
	}

}

func (t *Table) Commit() {

	if err := t.commit(); err != nil {
		log.Fatalf("Error table commit: %s.", err.Error())
	}
}
