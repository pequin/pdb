package pdb

import (
	"errors"
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

type Column interface {
	name() string
	datatype() string
}
type column struct {
	nme string
	tbl *Table
}

func (c *column) init(table *Table) error {

	if table == nil {
		return errors.New("pointer to table is null")
	}

	c.tbl = table

	return nil
}

func (c column) Int64(name string) *Int64 {

	clm := &Int64{}

	if err := clm.init(name, c.tbl); err != nil {
		log.Fatalf("Column: %s.", err.Error())
	}

	if err := c.tbl.Columns.append(clm); err != nil {
		log.Fatalf("Column: %s.", err.Error())
	}

	return clm
}
