package pdb

// package pdb

// import (
// 	"database/sql"
// 	"fmt"
// 	"log"
// 	"reflect"
// 	"strings"

// 	_ "github.com/lib/pq"
// 	"github.com/pequin/xlog"
// )

// type Connection struct {
// 	database *sql.DB
// }

// // type Get struct {
// // 	from    string
// // 	shema   string
// // 	columns []string
// // 	order   map[string]bool
// // 	limit   uint64
// // 	offset  uint64
// // }

// // type Set struct {
// // 	table   string
// // 	shema   string
// // 	columns []string
// // 	values  []any
// // }

// // func toPsqlColumnName(name string) string {
// // 	return strings.ReplaceAll(strings.ToLower(regexp.MustCompile(`([A-z])([A-Z][a-z])`).ReplaceAllString(name, `$1-$2`)), "-", "_")
// // }

// // func toPsqlColumnType(field reflect.Type) string {

// // 	switch field.Kind() {
// // 	case reflect.Float64:
// // 		return "numeric"
// // 	case reflect.ValueOf(time.Time{}).Kind():
// // 		return "timestamp without time zone"
// // 	}

// // 	return ""
// // }

// // Connect to the PostgreSQL database.
// func NewConnection(user, password, host, database string) *Connection {

// 	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=5432 user=%s password=%s dbname=%s sslmode=disable", host, user, password, database))
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	err = db.Ping()
// 	if err != nil {
// 		log.Fatalln(err)
// 	}

// 	return &Connection{db}
// }

// // func (c *Connection) Set(setter *Set) {
// // 	r := 1

// // 	sql := fmt.Sprintf("INSERT INTO %s.%s(%s) VALUES (", setter.shema, setter.table, strings.Join(setter.columns, ", "))

// // 	for i := 0; i < len(setter.values); i++ {

// // 		sql += fmt.Sprintf("$%d, ", i+1)

// // 		// End row.
// // 		if r == len(setter.columns) {
// // 			sql = strings.TrimSuffix(sql, ", ")
// // 			sql += "), ("
// // 			r = 0
// // 		}
// // 		r++
// // 	}

// // 	sql = strings.TrimSuffix(sql, ", (") + ";"

// // 	if len(setter.values) > 0 {

// // 		if _, err := c.database.Exec(sql, setter.values...); err != nil {
// // 			log.Fatalln(err)
// // 		}
// // 	}

// // }

// // func (c *Connection) Get(by *Get, row func(data ...any)) {

// // 	scan := make([]any, 0, len(by.columns))

// // 	rows, err := c.database.Query(by.string())
// // 	if err != nil {
// // 		log.Fatalln(err)
// // 	}

// // 	columnType, error := rows.ColumnTypes()
// // 	if error != nil {
// // 		log.Fatalln(error)
// // 	}

// // 	for i := 0; i < len(columnType); i++ {

// // 		switch columnType[i].DatabaseTypeName() {
// // 		case "INT2":
// // 			var smallint int16
// // 			scan = append(scan, &smallint)
// // 		case "INT4":
// // 			var integer int32
// // 			scan = append(scan, &integer)
// // 		case "INT8":
// // 			var bigint int64
// // 			scan = append(scan, &bigint)
// // 		case "FLOAT4":
// // 			var real float32
// // 			scan = append(scan, &real)
// // 		case "FLOAT8":
// // 			var doublePrecision float32
// // 			scan = append(scan, &doublePrecision)
// // 		case "NUMERIC":
// // 			var numeric float64
// // 			scan = append(scan, &numeric)
// // 		case "TIMESTAMP":
// // 			var timestamp time.Time
// // 			scan = append(scan, &timestamp)
// // 		case "BOOL":
// // 			var boolean bool
// // 			scan = append(scan, &boolean)
// // 		default:
// // 			var other any
// // 			scan = append(scan, &other)
// // 		}

// // 	}

// // 	for rows.Next() {

// // 		if err := rows.Scan(scan...); err != nil {
// // 			log.Fatal(err)
// // 		}

// // 		row(scan...)
// // 	}

// // 	defer rows.Close()
// // }

// // // Create schema if not exists.
// // func (c *Connection) Schema(name string) error {

// // 	_, err := c.database.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", name))

// // 	return err
// // }

// // // Create schema if not exists.
// // // func (c *Connection) Table(schema, name string, structure any) error {

// // // 	if err := c.Schema(schema); err != nil {
// // // 		return err
// // // 	}

// // // 	sql := "CREATE TABLE IF NOT EXISTS %s.%s ("

// // // 	tpe := reflect.TypeOf(structure)

// // // 	for i := 0; i < tpe.NumField(); i++ {

// // // 		sql += toPsqlColumnName(tpe.Field(i).Name) + " " + toPsqlColumnType(tpe.Field(i).Type) + " NOT NULL, "
// // // 	}

// // // 	sql = strings.TrimSuffix(sql, ", ")

// // // 	sql += ")"

// // // 	sql = fmt.Sprintf(sql, schema, name)

// // // 	_, err := c.database.Exec(sql)

// // // 	return err
// // // }

// // func NewSet(shema, table string, columns ...string) *Set {
// // 	return &Set{table, shema, columns, make([]any, 0)}
// // }

// // func (s *Set) Add(row ...any) {
// // 	s.values = append(s.values, row...)
// // }

// // func NewGet(shema, table string, columns ...string) *Get {
// // 	return &Get{table, shema, columns, make(map[string]bool), 0, 0}
// // }

// // func (g *Get) Sort(by string, ascending bool) {
// // 	g.order[by] = ascending
// // }

// // func (g *Get) Limit(limit uint64) {
// // 	g.limit = limit
// // }

// // func (g *Get) Offset(offset uint64) {
// // 	g.offset = offset
// // }

// // func (g *Get) string() string {

// // 	sql := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(g.columns, ", "), g.shema, g.from)

// // 	if len(g.order) > 0 {

// // 		sql += " ORDER BY"

// // 		for by, asc := range g.order {

// // 			sql += " " + by

// // 			if asc {
// // 				sql += " ASC"
// // 			} else {
// // 				sql += " DESC"
// // 			}

// // 			sql += ","
// // 		}

// // 		sql = strings.TrimSuffix(sql, ",")
// // 	}

// // 	if g.limit > 0 {
// // 		sql += fmt.Sprintf(" LIMIT %d", g.limit)
// // 	}
// // 	if g.offset > 0 {
// // 		sql += fmt.Sprintf(" OFFSET %d", g.offset)
// // 	}

// // 	return sql + ";"
// // }

// ///////////////////////////////////////////////////////

// type Type uint64

// const (
// 	Bool = iota
// 	Uint64
// 	Float64
// 	String
// 	Time
// )

// func (t *Type) SQL() string {

// 	switch *t {
// 	case Bool:
// 		return "BOOLEAN"
// 	case Uint64:
// 		return "BIGINT"
// 	case Float64:
// 		return "NUMERIC"
// 	case String:
// 		return "TEXT"
// 	case Time:
// 		return "TIMESTAMP WITHOUT TIME ZONE"
// 	default:
// 		return "CHAR"
// 	}
// }

// func (t *Type) Var() *any {

// 	switch *t {
// 	case Bool:
// 		return nil
// 	case String:
// 		return nil
// 	default:
// 		var other any
// 		return &other
// 	}
// }

// func typeFromReflect(tpe reflect.Type) Type {

// 	switch tpe.String() {
// 	case "string":
// 		return String
// 	case "time.Time":
// 		return Time
// 	default:
// 		return Bool
// 	}
// }

// type column struct {
// 	name     string
// 	datatype Type
// }

// type Table struct {
// 	name       string
// 	schema     string
// 	connection *Connection
// 	columns    []*column
// 	data       [][]any
// }

// func Column(name string, datatype Type) column {
// 	return column{name: name, datatype: datatype}
// }

// func (c *Connection) Table(schema, name string, columns ...*column) *Table {

// 	sql := "CREATE SCHEMA IF NOT EXISTS %[1]s; CREATE TABLE IF NOT EXISTS %[1]s.%[2]s (%[3]s)"

// 	cls := make([]string, len(columns))

// 	for i := 0; i < len(columns); i++ {
// 		cls[i] = fmt.Sprintf("%s %s NOT NULL", columns[i].name, columns[i].datatype.SQL())
// 	}

// 	sql = fmt.Sprintf(sql, schema, name, strings.Join(cls, ", "))

// 	_, err := c.database.Exec(sql)
// 	xlog.Fatalln(err)

// 	tbe := Table{name: name, schema: schema, connection: c, columns: columns, data: make([][]any, 0)}

// 	return &tbe
// }

// func (t *Table) AddRow(data ...any) {

// 	if len(t.columns) == len(data) {
// 		t.data = append(t.data, data)
// 	} else {
// 		xlog.Fatalln("The number of values must equal the number of columns.")
// 	}
// }

// func (t *Table) Commit() {

// 	// Values.
// 	vls := make([]any, 0)

// 	// Column names.
// 	cns := make([]string, len(t.columns))

// 	// Columns.
// 	cms := make([]string, len(t.columns))

// 	// Rows.
// 	rws := make([]string, len(t.data))

// 	dID := 1
// 	for rID := 0; rID < len(t.data); rID++ {

// 		for cID := 0; cID < len(t.data[rID]); cID++ {

// 			cms[cID] = fmt.Sprintf("$%d", dID)

// 			vls = append(vls, t.data[rID][cID])

// 			dID++
// 		}

// 		rws[rID] = fmt.Sprintf("(%s)", strings.Join(cms, ","))
// 	}

// 	for cID := 0; cID < len(t.columns); cID++ {
// 		cns[cID] = t.columns[cID].name
// 	}

// 	sql := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES %s", t.schema, t.name, strings.Join(cns, ","), strings.Join(rws, ","))

// 	_, err := t.connection.database.Exec(sql, vls...)
// 	xlog.Fatalln(err)

// 	t.data = t.data[:0]
// }

// type Select struct {
// 	table   *Table
// 	columns []*column
// 	order   map[*column]bool
// 	// ORDER BY
// }

// func (t *Table) Select(columns ...*column) *Select {

// 	slt := Select{table: t}

// 	// Is match.
// 	ism := false

// 	for cID := 0; cID < len(columns); cID++ {

// 		ism = false

// 		for id := 0; !ism && id < len(t.columns); id++ {
// 			ism = columns[cID] == t.columns[id]
// 		}

// 		if ism {
// 			slt.columns = append(slt.columns, columns[cID])
// 		} else {
// 			xlog.Fatallf("Column \"%s\" does not match the one specified in the table.", columns[cID].name)
// 		}

// 	}

// 	return &slt
// }

// func (s *Select) Rows() {

// 	cls := make([]string, len(s.columns))

// 	for i := 0; i < len(s.columns); i++ {
// 		cls[i] = s.columns[i].name
// 	}

// 	sql := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(cls, ", "), s.table.schema, s.table.name)

// 	rws, err := s.table.connection.database.Query(sql)
// 	xlog.Fatalln(err)
// 	defer s.table.connection.database.Close()
// 	defer rws.Close()

// 	// Column types.
// 	cts, err := rws.ColumnTypes()
// 	xlog.Fatalln(err)

// 	for i := 0; i < len(cts); i++ {

// 		dfb := typeGO(cts[i].ScanType())

// 		fmt.Println("ttt", dfb)

// 		// switch columnType[i].DatabaseTypeName() {
// 		// case "INT2":
// 		// 	var smallint int16
// 		// 	scan = append(scan, &smallint)
// 		// case "INT4":
// 		// 	var integer int32
// 		// 	scan = append(scan, &integer)
// 		// case "INT8":
// 		// 	var bigint int64
// 		// 	scan = append(scan, &bigint)
// 		// case "FLOAT4":
// 		// 	var real float32
// 		// 	scan = append(scan, &real)
// 		// case "FLOAT8":
// 		// 	var doublePrecision float32
// 		// 	scan = append(scan, &doublePrecision)
// 		// case "NUMERIC":
// 		// 	var numeric float64
// 		// 	scan = append(scan, &numeric)
// 		// case "TIMESTAMP":
// 		// 	var timestamp time.Time
// 		// 	scan = append(scan, &timestamp)
// 		// case "BOOL":
// 		// 	var boolean bool
// 		// 	scan = append(scan, &boolean)
// 		// default:
// 		// 	var other any
// 		// 	scan = append(scan, &other)
// 		// }

// 	}

// 	// scan := make([]any, len(s.columns))

// 	// // var scan any
// 	// // var ffff any

// 	// for rws.Next() {

// 	// 	if err := rws.Scan(&scan); err != nil {
// 	// 		log.Fatal(err)
// 	// 	}

// 	// 	fmt.Println("row:", scan)
// 	// }

// 	// 	columnType, error := rows.ColumnTypes()
// 	// 	if error != nil {
// 	// 		log.Fatalln(error)
// 	// 	}

// 	// fmt.Println("fff", sql, rows)

// 	// cls := make([]string, len(columns))

// 	//

// }

// // func (g *Get) string() string {

// // 	sql := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(g.columns, ", "), g.shema, g.from)

// // 	if len(g.order) > 0 {

// // 		sql += " ORDER BY"

// // 		for by, asc := range g.order {

// // 			sql += " " + by

// // 			if asc {
// // 				sql += " ASC"
// // 			} else {
// // 				sql += " DESC"
// // 			}

// // 			sql += ","
// // 		}

// // 		sql = strings.TrimSuffix(sql, ",")
// // 	}

// // 	if g.limit > 0 {
// // 		sql += fmt.Sprintf(" LIMIT %d", g.limit)
// // 	}
// // 	if g.offset > 0 {
// // 		sql += fmt.Sprintf(" OFFSET %d", g.offset)
// // 	}

// // 	return sql + ";"
// // }

// // func (c *Connection) Get(by *Get, row func(data ...any)) {

// // 	scan := make([]any, 0, len(by.columns))

// // 	rows, err := c.database.Query(by.string())
// // 	if err != nil {
// // 		log.Fatalln(err)
// // 	}

// // 	columnType, error := rows.ColumnTypes()
// // 	if error != nil {
// // 		log.Fatalln(error)
// // 	}

// // 	for i := 0; i < len(columnType); i++ {

// // 		switch columnType[i].DatabaseTypeName() {
// // 		case "INT2":
// // 			var smallint int16
// // 			scan = append(scan, &smallint)
// // 		case "INT4":
// // 			var integer int32
// // 			scan = append(scan, &integer)
// // 		case "INT8":
// // 			var bigint int64
// // 			scan = append(scan, &bigint)
// // 		case "FLOAT4":
// // 			var real float32
// // 			scan = append(scan, &real)
// // 		case "FLOAT8":
// // 			var doublePrecision float32
// // 			scan = append(scan, &doublePrecision)
// // 		case "NUMERIC":
// // 			var numeric float64
// // 			scan = append(scan, &numeric)
// // 		case "TIMESTAMP":
// // 			var timestamp time.Time
// // 			scan = append(scan, &timestamp)
// // 		case "BOOL":
// // 			var boolean bool
// // 			scan = append(scan, &boolean)
// // 		default:
// // 			var other any
// // 			scan = append(scan, &other)
// // 		}

// // 	}

// // 	for rows.Next() {

// // 		if err := rows.Scan(scan...); err != nil {
// // 			log.Fatal(err)
// // 		}

// // 		scan := make([]any, 0, len(by.columns))(scan...)
// // 	}

// // 	defer rows.Close()
// // }
