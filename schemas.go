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

type schemas struct {
	database *Database
}

func (s *schemas) init(database *Database) error {

	if database == nil {
		return errors.New("pointer to database is null")
	}

	s.database = database

	return nil
}

func (s *schemas) New(name string) *Schema {

	schema := &Schema{}

	if err := schema.init(name, s.database); err != nil {
		log.Fatalf("Error schemas new: %s.", err.Error())
	}

	return schema
}
