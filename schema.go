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

type Schema schema
type schema struct {
	nam    string    // Name.
	dbe    *Database // Database.
	Tables tables
}

func (s *Schema) init(name string, database *Database) error {

	name = strings.TrimSpace(name)

	if len(name) < 1 {
		return errors.New("name is not specified")
	}

	if database == nil {
		return errors.New("pointer to database is null")
	}

	s.nam = name
	s.dbe = database

	if err := s.Tables.init(s); err != nil {
		return err
	}

	return nil
}

func (s *Schema) Create() {

	if _, err := s.dbe.pgd.Exec(fmt.Sprintf("CREATE SCHEMA %s;", s.nam)); err != nil {
		log.Fatalf("Error schema create: %s.", err.Error())
	}
}
