package pdb

import (
	"context"
	"database/sql"
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

type data struct {
	tbl *Table
	stx *sql.Tx
}

func (d *data) init(table *Table) error {
	d.tbl = table

	return nil
}

func (d *data) begin() error {

	if d.stx == nil {

		stx, err := d.tbl.sma.dba.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
		if err != nil {
			return err
		}

		d.stx = stx
	}

	return nil
}

func (d *data) commit() error {
	if d.stx == nil {
		return fmt.Errorf("pointer to transaction is null")
	}

	if err := d.stx.Commit(); err != nil {

		if err := d.stx.Rollback(); err != nil {
			return err
		}

		return err
	}

	d.stx = nil

	return nil

}

func (d *data) Commit() {

	if err := d.commit(); err != nil {
		log.Fatalf("Error data commit: %s.", err.Error())
	}
}
