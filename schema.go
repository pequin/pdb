package pdb

import (
	"fmt"

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

type schema struct {
	nam string    // Schema name.
	iss bool      // The schema has already been initialized.
	dat *database // Database.
}

func (s *schema) table(name string) *table {
	tab := table{nam: name, sch: s}
	return &tab
}

func (s *schema) create() {

	fmt.Println("dfbfd", s)

	if !s.iss {
		_, err := s.dat.trx.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", s.nam))
		xlog.Fatalln(err)
		s.iss = true
	}
}