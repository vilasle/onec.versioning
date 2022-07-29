package main

import (
	// "database/sql"
	// "fmt"
	// "log"

	"database/sql"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/vilamslep/iokafka"
	pg "github.com/vilamslep/onec.versioning/postgres"
)

var (
	fref  string = "ref"
	fdate string = "date"
	fbool string = "bool"
	fstr  string = "string"
	fint  string = "int"
)

func main() {

	// config := iokafka.KafkaConfig{
	// 	Brokers: []string{"172.19.1.3:9092"},
	// 	Topic:   "raw",
	// 	GroupID: "processing",
	// 	AttemtsOnFail: 5,
	// }
	// rd := iokafka.NewScanner(config)

	// for rd.Scan() {

	// 	msg := rd.Message()

	// 	fmt.Println(msg)

	// }

	s := `{"#",7433d4e8-f07d-4ace-819f-00191a4913db,431:b3c1a4bf015829f711ed05ebb7aaf42c}`

	t := regexp.MustCompile(`[\d]{1,6}:[[:xdigit:]]{32}`)

	ns := t.FindString(s)

	if ns == "" {
		panic("empty ns")
	}

	conn, err := pg.NewConnection(confConnection())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	pos := strings.Split(ns, ":")
	tnum, ref := pos[0], pos[1]

	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	slt := psql.Select("t1.table_name as table, t2.field_name as field, t3.type_name as type").
		From("metadata_table_main as t1").
		LeftJoin("field as t2 on t1.id = t2.table_id").
		LeftJoin("field_type as t3 on t2.id = t3.field_id").
		Where(sq.Eq{"t1.table_number": tnum}).
		Where(sq.Eq{"t2.vt": false}).
		RunWith(conn)

	rows, err := slt.Query()

	if err != nil {
		panic(err)
	}

	rs, err := readRows(rows)

	if err != nil {
		panic(err)
	}

	if len(rs) == 0 {
		panic("not setting for this table")
	}

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s", "192.168.4.230", "sa", "1111", 1433, "pre-prod")

	mconn, err := sql.Open("mssql", connString)
	if err != nil {
		panic(err)
	}
	defer mconn.Close()

	fields := make(map[string]string)
	tbNm := "dbo." + rs[0]["table"]

	for _, fl := range rs {
		fields[fl["field"]] = fl["type"]
	}

	var scols string
	for k := range fields {
		scols += fmt.Sprintf("t1.%s,", k)
	}

	scols = scols[:len(scols)-2]

	slms := sq.Select(scols).
		From(tbNm + " AS t1").
		Where(sq.Eq{"CONVERT(VARCHAR(34), t1._IDRRef, 2)": ref}).
		RunWith(mconn)

	mrows, err := slms.Query()

	if err != nil {
		panic(err)
	}
	// mrs
	_, err = readRows(mrows)

	if err != nil {
		panic(err)
	}
}

func readRows(rows *sql.Rows) ([]map[string]string, error) {

	cols, err := rows.Columns()

	if err != nil {
		return nil, err
	}

	rs := make([]map[string]string, 0)

	forScn := createSliceForScanning(cols)

	for rows.Next() {

		if err := rows.Scan(forScn[:]...); err != nil {
			return nil, err
		}

		row := make(map[string]string)

		for i := range forScn {
			val := *forScn[i].(*interface{})
			row[cols[i]] = getColumnValueAsString(val)
		}

		rs = append(rs, row)
	}

	return rs, nil

}

func getColumnValueAsString(val interface{}) (res string) {

	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case []byte:
		res = string(v)
	default:
		res = fmt.Sprintf("%v", v)
	}

	return res
}

func createSliceForScanning(cols []string) []interface{} {
	results := make([]interface{}, len(cols))
	for i := range results {
		results[i] = new(interface{})
	}
	return results
}

func confConnection() pg.PGAuth {
	return pg.PGAuth{
		User:     "onecversion",
		Password: "onecversion",
		Host:     "172.19.2.2",
		Port:     5432,
		SslMode:  false,
		Dbname:   "onecversion",
	}
}
