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

type Float64 column

func (f *Float64) init(name string, table *Table) error {
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

func (c column) Float64(name string) *Float64 {

	cun := &Float64{}

	if err := cun.init(name, c.tbe); err != nil {
		log.Fatalf("Error column float64: %s.", err.Error())
	}

	if err := c.tbe.Columns.append(cun); err != nil {
		log.Fatalf("Error column float64: %s.", err.Error())
	}

	return cun
}

// Implementation of the "Column" interface.
func (f *Float64) name() string {
	return f.nam
}

// Implementation of the "Column" interface.
func (Float64) as() string {
	return "NUMERIC"
}

// Implementation of the "Column" interface.
func (f *Float64) table() *Table {
	return f.tbe
}

func (f *Float64) Listen(value *float64) listener {
	if value == nil {
		log.Fatalf("Error float64 create listen for column with name \"%s\": pointer to value is null.", f.nam)
	}
	return listener{cun: f, ber: value}
}

func (f *Float64) Insert(value float64) insert {
	return insert{cun: f, vue: value}
}

func (f *Float64) Is(value float64) is {
	return is{cun: f, vue: fmt.Sprintf("%f", value)}
}
