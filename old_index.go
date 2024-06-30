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

type old_index struct {
	nme string // Name of column.
	opr string // Operator.
	val string // Value.
}

func (i *old_index) NewFilter() *old_filter {
	return &old_filter{idx: i}
}

func (i *old_index) less(value string) *old_index {
	i.opr = "<"
	i.val = value
	return i
}
func (i *old_index) lessOrEqual(value string) *old_index {
	i.opr = "<="
	i.val = value
	return i
}

// Seteds operator "=" for custom value and returns pointer to object where.
func (i *old_index) equal(value string) *old_index {
	i.opr = "="
	i.val = value
	return i
}

// Seteds operator "<>" for custom value and returns pointer to object where.
func (i *old_index) notEqual(value string) *old_index {
	i.opr = "<>"
	i.val = value
	return i
}

// Seteds operator ">" for custom value and returns pointer to object where.
func (i *old_index) greater(value string) *old_index {
	i.opr = ">"
	i.val = value
	return i
}

// Seteds operator ">=" for custom value and returns pointer to object where.
func (i *old_index) greaterOrEqual(value string) *old_index {
	i.opr = ">="
	i.val = value
	return i
}
