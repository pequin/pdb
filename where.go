package pdb

import (
	"fmt"
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

type Where where
type where struct {
	cln Column // Column interface.
	cmn string // Comparison.
	vle string // Value.
}

func (w *Where) update(i is) error {
	if w.cln.table() != i.cun.table() {
		return fmt.Errorf("column with name \"%s\" refers to the table \"%s\" and column with name \"%s\" refers to the table \"%s\"", w.cln.name(), w.cln.table().nam, i.cun.name(), i.cun.table().nam)
	}
	if w.cln != i.cun {
		return fmt.Errorf("column with name \"%s\" do not match with column \"%s\" in where", w.cln.name(), i.cun.name())
	}
	w.vle = i.vue

	return nil
}

func (w *Where) Is(i is) *Where {
	if err := w.update(i); err != nil {
		log.Fatalf("Error where is: %s.", err.Error())
	}
	return w
}
