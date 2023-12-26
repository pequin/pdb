package pdb

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type Connection struct {
	database *sql.DB
}

type Get struct {
	from    string
	shema   string
	columns []string
	order   map[string]bool
	limit   uint64
	offset  uint64
}

type Set struct {
	to      string
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

func (c *Connection) Set(setter *Set) {
	r := 1

	sql := fmt.Sprintf("INSERT INTO %s.%s(%s) VALUES (", setter.shema, setter.to, strings.Join(setter.columns, ", "))

	for i := 0; i < len(setter.values); i++ {

		sql += fmt.Sprintf("$%d, ", i+1)

		// End row.
		if r == len(setter.columns) {
			sql = strings.TrimSuffix(sql, ", ")
			sql += "), ("
			r = 0
		}
		r++
	}

	sql = strings.TrimSuffix(sql, ", (") + ";"

	if _, err := c.database.Exec(sql, setter.values...); err != nil {
		log.Fatalln(err)
	}
}

func (c *Connection) Get(by *Get, row func(data ...any)) {

	scan := make([]any, 0, len(by.columns))

	rows, err := c.database.Query(by.string())
	if err != nil {
		log.Fatalln(err)
	}

	columnType, error := rows.ColumnTypes()
	if error != nil {
		log.Fatalln(error)
	}

	for i := 0; i < len(columnType); i++ {

		// fmt.Print("ttt", columnType[i].DatabaseTypeName())

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
		case "TIMESTAMP":
			var timestamp time.Time
			scan = append(scan, &timestamp)
		case "BOOL":
			var boolean bool
			scan = append(scan, &boolean)
		default:
			// fmt.Print("defaultdefaultdefaultdefault", columnType[i].DatabaseTypeName())
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

	defer rows.Close()
}

func NewSet(to string, columns ...string) *Set {
	return &Set{to, "public", columns, make([]any, 0)}
}

func (s *Set) Add(row ...any) {
	s.values = append(s.values, row...)
}

func NewGet(from string, columns ...string) *Get {
	return &Get{from, "public", columns, make(map[string]bool), 0, 0}
}

func (g *Get) Sort(by string, ascending bool) {
	g.order[by] = ascending
}

func (g *Get) Limit(limit uint64) {
	g.limit = limit
}

func (g *Get) Offset(offset uint64) {
	g.offset = offset
}

func (g *Get) string() string {

	sql := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(g.columns, ", "), g.shema, g.from)

	if len(g.order) > 0 {

		sql += " ORDER BY"

		for by, asc := range g.order {

			sql += " " + by

			if asc {
				sql += " ASC"
			} else {
				sql += " DESC"
			}

			sql += ","
		}

		sql = strings.TrimSuffix(sql, ",")
	}

	if g.limit > 0 {
		sql += fmt.Sprintf(" LIMIT %d", g.limit)
	}
	if g.offset > 0 {
		sql += fmt.Sprintf(" OFFSET %d", g.offset)
	}

	return sql + ";"
}
