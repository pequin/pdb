package pdb

import (
	"errors"
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

type String column

func (f *String) init(name string, table *Table) error {
	name = strings.TrimSpace(name)

	if len(name) < 1 {
		return errors.New("name is not specified")
	}

	if table == nil {
		return errors.New("pointer to table is null")
	}

	f.nam = name
	f.tbe = table

	return nil
}

func (c column) String(name string) *String {

	cun := &String{}

	if err := cun.init(name, c.tbe); err != nil {
		log.Fatalf("Error column string: %s.", err.Error())
	}

	if err := c.tbe.Columns.append(cun); err != nil {
		log.Fatalf("Error column string: %s.", err.Error())
	}

	return cun
}

// Implementation of the "Column" interface.
func (s *String) name() string {
	return s.nam
}

// Implementation of the "Column" interface.
func (String) as() string {
	return "TEXT"
}

// Implementation of the "Column" interface.
func (s *String) table() *Table {
	return s.tbe
}

func (s *String) Listen(value *string) listener {
	if value == nil {
		log.Fatalf("Error string create listen for column with name \"%s\": pointer to value is null.", s.nam)
	}
	return listener{cun: s, ber: value}
}

func (s *String) Insert(value string) insert {
	return insert{cun: s, vue: value}
}

func (s *String) Is(value string) is {
	return is{cun: s, vue: value}
}
