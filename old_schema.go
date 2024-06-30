package pdb

import (
	"context"
	"database/sql"
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

type old_schema struct {
	nme string    // Name.
	con *sql.Conn // Connection to database.
}

func (d *old_database) Schema(name string) *old_schema {

	con, err := d.dba.Conn(context.Background())
	xlog.Fatalln(err)
	return &old_schema{nme: strings.ToLower(name), con: con}
}
