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

type is struct {
	cun Column // Column.
	vue string // Value.
}

func (i is) Less() *Where {
	return &Where{cln: i.cun, cmn: "<", vle: i.vue}
}

func (i is) LessOrEqual() *Where {
	return &Where{cln: i.cun, cmn: "<=", vle: i.vue}
}

func (i is) Equal() *Where {
	return &Where{cln: i.cun, cmn: "=", vle: i.vue}
}

func (i is) NotEqual() *Where {
	return &Where{cln: i.cun, cmn: "<>", vle: i.vue}
}

func (i is) Greater() *Where {
	return &Where{cln: i.cun, cmn: ">", vle: i.vue}
}

func (i is) GreaterOrEqual() *Where {
	return &Where{cln: i.cun, cmn: ">=", vle: i.vue}
}
