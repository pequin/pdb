package pdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pequin/xlog"
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

type old_table struct {
	Type types
	Read old_reader
	nme  string      // Name.
	sdb  *old_schema // Schema of database.
	stx  *sql.Tx     // Transaction
	isi  bool        // This table is init

	// Writer.
	wrt struct {
		qry string //Query
		row []any
	}
}

func (s *old_schema) Table(name string) *old_table {
	t := &old_table{nme: strings.ToLower(name), sdb: s, isi: false}
	t.Type.tbl = t
	t.Read.tbl = t
	t.begin()
	return t
}

// Commit commits the transaction.
func (t *old_table) Commit() {

	if t.stx != nil {

		t.init()
		t.stx.Commit()
	}

	t.begin()
}

// Returns the string which includes the name of the scheme and table name.
func (t *old_table) name() string {
	return fmt.Sprintf("%s.%s", t.sdb.nme, t.nme)
}

func (t *old_table) init() {

	if !t.isi {

		cl := len(t.Type.cls)

		bl := cl // Buffer length.

		if t.Type.ser != nil {
			bl++
		}

		hdr := make([]string, cl) // Headers.
		iio := make([]string, cl) // Variables for insertinto.

		t.Read.buf = make([]any, bl)

		if t.Type.ser != nil {
			t.Read.buf[0] = t.Type.ser.buffer()
		}

		hap := make([]string, 0) // Headers and primary keys.

		if t.Type.ser != nil {
			hap = append(hap, fmt.Sprintf("%s %s", t.Type.ser.name(), t.Type.ser.sql()))
		}

		for i := 0; i < cl; i++ {
			hdr[i] = t.Type.cls[i].name()
			iio[i] = fmt.Sprintf("$%d", i+1)
			hap = append(hap, fmt.Sprintf("%s %s NOT NULL", hdr[i], t.Type.cls[i].sql()))

			if t.Type.ser == nil {

				t.Read.buf[i] = t.Type.cls[i].buffer()
			} else {

				t.Read.buf[i+1] = t.Type.cls[i].buffer()
			}
		}

		if p := t.Type.primary(); len(p) > 0 {
			hap = append(hap, fmt.Sprintf("PRIMARY KEY(%s)", strings.Join(p, ", ")))
		}

		qry := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s; CREATE TABLE IF NOT EXISTS %s (%s);", t.sdb.nme, t.name(), strings.Join(hap, ", "))

		_, err := t.stx.Exec(qry)
		xlog.Fatalln(err)

		t.Type.createIndexes()

		// Reader.
		t.Read.cls = strings.Join(hdr, ", ")

		if t.Type.ser != nil {
			t.Read.cls = fmt.Sprintf("%s, %s", t.Type.ser.name(), t.Read.cls)
		}

		// Writer.
		t.wrt.qry = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", t.name(), strings.Join(hdr, ", "), strings.Join(iio, ", "))
		t.wrt.row = make([]any, cl)

		t.isi = true
	}

}

// Begin starts a transaction and rolls back the previous transaction.
func (t *old_table) begin() {
	if t.stx != nil {
		t.stx.Rollback()
	}
	stx, err := t.sdb.con.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
	xlog.Fatalln(err)
	t.stx = stx
}

func (t *old_table) write(to Type, value any) {

	t.init()

	nfc := 0 // Number of filled columns.

	for i := 0; i < len(t.Type.cls); i++ {

		if t.Type.cls[i] == to && t.wrt.row[i] == nil {
			t.wrt.row[i] = value
		}

		if t.wrt.row[i] != nil {
			nfc++
		}
	}

	if nfc == len(t.Type.cls) {
		t.writer()
	}
}

func (t *old_table) writer() {
	_, err := t.stx.Exec(t.wrt.qry, t.wrt.row...)
	xlog.Fatalln(err)

	for i := 0; i < len(t.wrt.row); i++ {
		t.wrt.row[i] = nil
	}
}
