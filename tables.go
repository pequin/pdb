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

type tables struct {
	sea *Schema // Schema.
}

func (t *tables) init(schema *Schema) error {

	if schema == nil {
		return errors.New("pointer to schema is null")
	}

	t.sea = schema

	return nil
}

func (t *tables) New(name string) *Table {

	table := &Table{}

	if err := table.init(name, t.sea); err != nil {
		log.Fatalf("Error tables new: %s.", err.Error())
	}

	return table
}
