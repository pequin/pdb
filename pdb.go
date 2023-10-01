package pdb

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq"
)

type Connection struct {
	database *sql.DB
}

type Table struct {
	name    string
	shema   string
	columns []string
	values  []any
}

// Connect to the PostgreSQL database.
func NewConnection(user, password, host, database string) *Connection {

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=5432 user=%s password=%s dbname=%s sslmode=disable", host, user, password, database))
	if err != nil {
		log.Fatal(err)
	}
	err = db.Ping()
	if err != nil {
		log.Fatalln(err)
	}

	return &Connection{db}
}

// The NewInset function allocates and initializes an pointer to object of type Inset.
func NewTable(table string, columns ...string) *Table {

	return &Table{table, "public", columns, make([]any, 0)}
}

// Adds a new row.
func (t *Table) AddRow(values ...any) {

	t.values = append(t.values, values...)
}

func (c *Connection) Inset(table *Table) {

	r := 1

	sql := fmt.Sprintf("INSERT INTO %s.%s(%s) VALUES (", table.shema, table.name, strings.Join(table.columns, ", "))

	for i := 0; i < len(table.values); i++ {

		sql += fmt.Sprintf("$%d, ", i+1)

		// End row.
		if r == len(table.columns) {
			sql = strings.TrimSuffix(sql, ", ")
			sql += "), ("
			r = 0
		}
		r++
	}

	sql = strings.TrimSuffix(sql, ", (") + ";"

	if _, err := c.database.Exec(sql, table.values...); err != nil {
		log.Fatalln(err)
	}
}

func (c *Connection) Select(row func(dest ...any), table string, columns ...string) {

	scan := make([]any, 0, len(columns))

	sql := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(columns, ", "), "public", table)

	rows, err := c.database.Query(sql)
	if err != nil {
		log.Fatalln(err)
	}

	columnType, error := rows.ColumnTypes()
	if error != nil {
		log.Fatalln(error)
	}

	for i := 0; i < len(columnType); i++ {

		switch columnType[i].DatabaseTypeName() {
		case "INT2":
			var smallint int16
			scan = append(scan, &smallint)
		case "INT4":
			var integer int32
			scan = append(scan, &integer)
		case "INT8":
			var bigint int64
			scan = append(scan, &bigint)
		case "FLOAT4":
			var real float32
			scan = append(scan, &real)
		case "FLOAT8":
			var doublePrecision float32
			scan = append(scan, &doublePrecision)
		case "NUMERIC":
			var numeric float64
			scan = append(scan, &numeric)
		default:
			var other any
			scan = append(scan, &other)
		}

	}

	for rows.Next() {

		if err := rows.Scan(scan...); err != nil {
			log.Fatal(err)
		}

		row(scan...)
	}
}
