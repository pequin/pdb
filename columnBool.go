package pdb

import (
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

type Bool column

func (b *Bool) init(name string, table *Table) error {
	name = strings.TrimSpace(name)

	if len(name) < 1 {
		return errors.New("name is not specified")
	}

	if table == nil {
		return errors.New("pointer to table is null")
	}

	b.nam = name
	b.tbe = table

	return nil
}

func (c column) Bool(name string) *Bool {

	cun := &Bool{}

	if err := cun.init(name, c.tbe); err != nil {
		log.Fatalf("Error column bool: %s.", err.Error())
	}

	if err := c.tbe.Columns.append(cun); err != nil {
		log.Fatalf("Error column bool: %s.", err.Error())
	}

	return cun
}

// Implementation of the "Column" interface.
func (b *Bool) name() string {
	return b.nam
}

// Implementation of the "Column" interface.
func (Bool) as() string {
	return "BOOLEAN"
}

// Implementation of the "Column" interface.
func (b *Bool) table() *Table {
	return b.tbe
}

func (b *Bool) Listen(value *bool) listener {
	if value == nil {
		log.Fatalf("Error bool create listen for column with name \"%s\": pointer to value is null.", b.nam)
	}
	return listener{cun: b, ber: value}
}

func (b *Bool) Insert(value bool) insert {
	return insert{cun: b, vue: value}
}

func (b *Bool) Is(value bool) is {
	return is{cun: b, vue: fmt.Sprintf("%t", value)}
}
