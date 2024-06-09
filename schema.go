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

type schema struct {
	nme string // Name.
	// dba *database // Database.
}

func (d *database) Schema(name string) *schema {
	// _, err := d.dba.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", name))
	// xlog.Fatalln(err)
	// return &schema{nme: name, dba: d}
	return &schema{nme: name}
}

// func (s *schema) Table(name string) *table {
// 	return &table{nm: name, sh: s}
// }
