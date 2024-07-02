package pdb

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
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

type Time column

func (t *Time) init(name string, table *Table) error {
	name = strings.TrimSpace(name)

	if len(name) < 1 {
		return errors.New("name is not specified")
	}

	if table == nil {
		return errors.New("pointer to table is null")
	}

	t.nam = name
	t.tbe = table

	return nil
}

func (c column) Time(name string) *Time {

	cun := &Time{}

	if err := cun.init(name, c.tbe); err != nil {
		log.Fatalf("Error column time: %s.", err.Error())
	}

	if err := c.tbe.Columns.append(cun); err != nil {
		log.Fatalf("Error column time: %s.", err.Error())
	}

	return cun
}

// Implementation of the "Column" interface.
func (t *Time) name() string {
	return t.nam
}

// Implementation of the "Column" interface.
func (Time) as() string {
	return "TIMESTAMP WITHOUT TIME ZONE"
}

// Implementation of the "Column" interface.
func (t *Time) table() *Table {
	return t.tbe
}

func (t *Time) Listen(value *time.Time) listener {
	if value == nil {
		log.Fatalf("Error time create listen for column with name \"%s\": pointer to value is null.", t.nam)
	}
	return listener{cun: t, ber: value}
}

func (t *Time) Insert(value time.Time) insert {
	return insert{cun: t, vue: value.UTC()}
}

func (t *Time) Is(value time.Time) is {
	return is{cun: t, vue: fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%d", value.Year(), value.Month(), value.Day(), value.Hour(), value.Minute(), value.Second(), value.Nanosecond())}
}
