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

type sorting struct {
	cun []Column
	lis []bool
}

func (s *sorting) add(cun Column, lis bool) error {

	for i := 0; i < len(s.cun); i++ {

		if s.cun[i] == cun {
			return fmt.Errorf("column \"%s\" already is exist", cun.name())
		}
	}

	s.cun = append(s.cun, cun)
	s.lis = append(s.lis, lis)

	return nil
}

var errOrderIsEmpty = errors.New("order is empty")

func (s *sorting) order() (string, error) {

	if len(s.cun) < 1 {
		return "", errOrderIsEmpty
	}

	if len(s.cun) != len(s.lis) {
		return "", errors.New("data integrity is compromised")
	}

	str := make([]string, len(s.cun))

	for i := 0; i < len(s.cun); i++ {

		str[i] = fmt.Sprintf("%s.%s.%s %s", s.cun[i].table().sma.nam, s.cun[i].table().nam, s.cun[i].name(), strings.NewReplacer("true", "ASC", "false", "DESC").Replace(fmt.Sprintf("%t", s.lis[i])))

	}

	return fmt.Sprintf("ORDER BY %s", strings.Join(str, ", ")), nil
}

func (s *sorting) Asc(by Column) *sorting {
	if err := s.add(by, true); err != nil {
		log.Fatalf("Error sorting asc: %s.", err.Error())
	}
	return s
}

func (s *sorting) Desc(by Column) *sorting {
	if err := s.add(by, false); err != nil {
		log.Fatalf("Error sorting asc: %s.", err.Error())
	}
	return s
}
