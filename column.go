package pdb

import (
	"errors"
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

type Column interface {
	name() string  // Name.
	as() string    // PostgreSQL data type.
	table() *Table // Related table.
}
type column struct {
	nam string // Name.
	tbe *Table // Related table.
}

type insert struct {
	cun Column
	vue any
}

type listener struct {
	cun Column
	ber any
}

func (c *column) init(tbe *Table) error {

	if tbe == nil {
		return errors.New("pointer to table is null")
	}

	c.tbe = tbe

	return nil
}
