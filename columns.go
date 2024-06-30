package pdb

import (
	"errors"
	"fmt"
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

type columns struct {
	New column
	tbl *Table
	hdr map[Column]int
}

func (c *columns) init(table *Table) error {
	if table == nil {
		return errors.New("pointer to table is null")
	}
	c.tbl = table
	c.hdr = make(map[Column]int)

	if err := c.New.init(c.tbl); err != nil {
		return err
	}
	return nil
}

func (c *columns) len() int {
	return len(c.hdr)
}

func (c *columns) header() ([]string, error) {

	if c.len() < 1 {
		return nil, fmt.Errorf("header of table \"%s\" is empty", c.tbl.nme)
	}

	hdr := make([]string, c.len())

	for clm, idx := range c.hdr {
		hdr[idx] = clm.name()
	}

	return hdr, nil
}

func (c *columns) index(column Column) (int, error) {

	idx, ise := c.hdr[column]
	if ise {
		return idx, nil
	}

	return -1, fmt.Errorf("column in table \"%s\" not found", c.tbl.nme)
}

func (c *columns) column(index int) (Column, error) {

	for clm, idx := range c.hdr {
		if idx == index {
			return clm, nil
		}
	}

	return nil, fmt.Errorf("column in table \"%s\" at index \"%d\" not found", c.tbl.nme, index)
}

func (c *columns) datatypes() ([]string, error) {

	if c.len() < 1 {
		return nil, fmt.Errorf("header of table \"%s\" is empty", c.tbl.nme)
	}

	t := make([]string, c.len())

	for clm, i := range c.hdr {
		t[i] = fmt.Sprintf("%s %s NOT NULL", clm.name(), clm.datatype())
	}

	return t, nil
}

func (c *columns) append(column Column) error {

	if _, ise := c.hdr[column]; ise {
		return fmt.Errorf("column \"%s\" already exists", column.name())
	}

	for clm := range c.hdr {
		if clm.name() == column.name() {
			return fmt.Errorf("column \"%s\" already exists", column.name())
		}
	}

	c.hdr[column] = c.len()

	return nil
}
