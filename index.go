package pdb

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

// Creates and returms pointer to buffer.
// func (Bool) buf() any {
// 	return new(bool)
// }

type index struct {
	nme string // Name of column.
	opr string // Operator.
	val string // Value.
}

func (i *index) NewFilter() *filter {
	return &filter{idx: i}
}

func (i *index) less(value string) *index {
	i.opr = "<"
	i.val = value
	return i
}
func (i *index) lessOrEqual(value string) *index {
	i.opr = "<="
	i.val = value
	return i
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (i *index) equal(value string) *index {
	i.opr = "="
	i.val = value
	return i
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (i *index) notEqual(value string) *index {
	i.opr = "<>"
	i.val = value
	return i
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (i *index) greater(value string) *index {
	i.opr = ">"
	i.val = value
	return i
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (i *index) greaterOrEqual(value string) *index {
	i.opr = ">="
	i.val = value
	return i
}
