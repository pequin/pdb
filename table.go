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

type Table table
type table struct {
	nam     string  // Name of table.
	sma     *Schema // Schema.
	Columns columns
	Data    data
}

func (t *Table) init(name string, schema *Schema) error {

	name = strings.TrimSpace(name)

	if len(name) < 1 {
		return errors.New("name is not specified")
	}

	if schema == nil {
		return errors.New("pointer to schema is null")
	}

	t.nam = name
	t.sma = schema

	if err := t.Columns.init(t); err != nil {
		return err
	}
	if err := t.Data.init(t); err != nil {
		return err
	}

	return nil
}

func (t *Table) Create() {

	dts, err := t.Columns.datatypes()

	if err != nil {
		log.Fatalf("Error table create: %s.", err.Error())
	}

	if _, err := t.sma.dbe.pgd.Exec(fmt.Sprintf("CREATE TABLE %s.%s (%s)", t.sma.nam, t.nam, strings.Join(dts, ", "))); err != nil {
		log.Fatalf("Error table create: %s.", err.Error())
	}
}
