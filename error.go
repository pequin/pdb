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

// type Error struct {
// 	s string // Name of structure.
// 	m string // Name of method.
// 	e error  // Error.
// }

// func (e Error) Error() string {
// 	l := make([]string, 0)
// 	if len(e.s) > 0 {
// 		l = append(l, fmt.Sprintf("%s", strings.ToLower(e.s)))
// 	}
// 	if len(e.m) > 0 {
// 		l = append(l, fmt.Sprintf("%s", strings.ToLower(e.m)))
// 	}
// 	if e.e != nil {
// 		l = append(l, fmt.Sprintf("%s", strings.ToLower(e.e.Error())))
// 	}
// 	return strings.Join(l, " > ")
// }
