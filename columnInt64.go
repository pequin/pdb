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

type Int64 column

func (i *Int64) init(name string, table *Table) error {
	name = strings.TrimSpace(name)

	if len(name) < 1 {
		return errors.New("int64 init: name is not specified")
	}

	if table == nil {
		return errors.New("int64 init: pointer to table is null")
	}

	i.nam = name
	i.tbe = table

	return nil
}

// Implementation of the "Column" interface.
func (i *Int64) name() string {
	return i.nam
}

// Implementation of the "Column" interface.
func (Int64) as() string {
	return "BIGINT"
}

// Implementation of the "Column" interface.
func (i *Int64) table() *Table {
	return i.tbe
}

func (i *Int64) Listen(value *int64) Listener {
	if value == nil {
		log.Fatalf("Error int64 create listen for column with name \"%s\": pointer to value is null.", i.nam)
	}
	return Listener{cun: i, ber: value}
}

func (i *Int64) Is(value int64) is {
	return is{cun: i, vue: fmt.Sprintf("%d", value)}
}
