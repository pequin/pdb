package pdb

import (
	_ "github.com/lib/pq"
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

// type Connect struct {
// }

// // Connect to the PostgreSQL database.
// func Connectgggg(user, password, host, name string) *Database {

// 	// Database Connection.
// 	con, err := sql.Open("postgres", fmt.Sprintf("host=%s port=5432 user=%s password=%s dbname=%s sslmode=disable", host, user, password, name))
// 	xlog.Fatalln(err)

// 	xlog.Fatalln(con.Ping())

// 	dat := Database{con: con, isb: false}
// 	dat.begin()

// 	return &dat
// }
