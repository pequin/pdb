package pdb

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
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

type database struct {
	dba *sql.DB // Database.
	// *sql.Tx // Transaction.
	// nme string  // Name.
	// usr string  // User.
	// pwd string  // Password.
	// hst string  // Host.
	// prt uint64  // Port.

	// trx *sql.Tx // Transaction.
	// isb bool    // Is bnegin started a transaction.
}

// Connect to the PostgreSQL database.
func Database(name, user, password, host string, port uint64) *database {
	d, e := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, name))
	xlog.Fatalln(e)
	return &database{dba: d}
}

type thread struct {
	dba *sql.DB         // Database.
	ctx context.Context // Context.
	trx *sql.Tx         // Transaction.
}

// Created new thread for transaction.
func (d *database) Thread() *thread {
	return &thread{dba: d.dba, ctx: context.Background()}
}

func (t *thread) begin() {

	if t.trx == nil {
		trx, err := t.dba.BeginTx(t.ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: false})
		xlog.Fatalln(err)
		t.trx = trx
	}
}
