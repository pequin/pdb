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

type SQL struct {
	*database
	*schema
}

func (d *SQL) Connect(user, password, host, database, shema string) {
	d.database = connect(user, password, host, database)
	d.schema = d.database.schema(shema)
}

func (d *SQL) Table(name string) *table {
	return d.schema.table(name)
}
func (d *SQL) Commit() {
	d.database.commit()
}
