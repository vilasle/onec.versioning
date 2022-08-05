package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/vilamslep/iokafka"
	msql "github.com/vilamslep/onec.versioning/mssql"
	pg "github.com/vilamslep/onec.versioning/postgres"
)

type Version struct {
	Main map[string]string
	VT   map[string][]map[string]string
}

var (
	fref  string = "ref"
	fdate string = "date"
	fbool string = "bool"
	fstr  string = "string"
	fint  string = "int"
)

func getMainFields(tableNumber string, conn *sql.DB) ([]map[string]string, error) {

	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	slt := psql.Select("t1.table_name as table, t2.field_name as field, t3.type_name as type").
		From("metadata_table_main as t1").
		LeftJoin("field as t2 on t1.id = t2.table_id_main").
		LeftJoin("field_type as t3 on t2.id = t3.field_id").
		Where(sq.Eq{"t1.table_number": tableNumber}).
		RunWith(conn)

	if rows, err := slt.Query(); err == nil {
		return readRows(rows)
	} else {
		return nil, err
	}
}

func main() {

	/*// config := iokafka.KafkaConfig{
	// 	Brokers: []string{"172.19.1.3:9092"},
	// 	Topic:   "raw",
	// 	GroupID: "processing",
	// 	AttemtsOnFail: 5,
	// }
	// rd := iokafka.NewScanner(config)

	// for rd.Scan() {

	// 	msg := rd.Message()

	// 	fmt.Println(msg)

	// }*/

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

	mconn, err := msql.NewConnection(msql.MSSQLAuth{})
	if err != nil {
		panic(err)
	}
	defer mconn.Close()

	rs, err := getMainFields(tnum, conn)
	if err != nil {
		panic(err)
	}
	fields := make(map[string]string)
	maintb := rs[0]["table"]
	tbNm := "dbo." + maintb

	for _, fl := range rs {
		fields[fl["field"]] = fl["type"]
	}

	var scols string
	for k := range fields {
		var exec string
		switch fields[k] {
		case fref:
			exec = refExec(k)
		case fbool:
			exec = boolExec(k)
		case fdate:
			exec = dateExec(k)
		case fint:
			exec = intExec(k)
		case fstr:
			exec = stringExec(k)
		default:
			panic("unknowing field type")
		}

		scols += fmt.Sprintf("%s,", exec)
	}

	scols = scols[:len(scols)-1]

	slms := sq.Select(scols).
		From(tbNm + " AS t1").
		Where(sq.Eq{"CONVERT(VARCHAR(34), t1._IDRRef, 2)": ref}).
		RunWith(mconn)

	mrows, err := slms.Query()

	if err != nil {
		panic(err)
	}

	// mrs
	mrs, err := readRows(mrows)

	if len(mrs) == 0 || len(mrs) > 1 {
		panic("wrong")
	}

	version := Version{
		Main: mrs[0],
		VT:   make(map[string][]map[string]string),
	}

	vtlt := psql.Select("t1.table_name as table, t2.field_name as field, t3.type_name as type").
		From("metadata_table_vt as t1").
		LeftJoin("field as t2 on t1.id = t2.table_id_vt").
		LeftJoin("field_type as t3 on t2.id = t3.field_id").
		Where(sq.Eq{"t1.table_number": tnum}).
		RunWith(conn)

	vtrows, err := vtlt.Query()

	if err != nil {
		panic(err)
	}

	vtrs, err := readRows(vtrows)

	if err != nil {
		panic(err)
	}

	if len(rs) == 0 {
		panic("not setting for this table")
	}

	childTabs := make(map[string]map[string]string)
	for _, vti := range vtrs {
		table := vti["table"]
		fld := vti["field"]
		tfld := vti["type"]
		if _, ok := childTabs[table]; !ok {
			childTabs[table] = make(map[string]string, 0)
		}
		childTabs[vti["table"]][fld] = tfld
	}

	for k, v := range childTabs {
		fields := make(map[string]string)
		tbNm := "dbo." + k

		for flk, flv := range v {
			fields[flk] = flv
		}

		var scols string
		for k := range fields {
			var exec string
			switch fields[k] {
			case fref:
				exec = refExec(k)
			case fbool:
				exec = boolExec(k)
			case fdate:
				exec = dateExec(k)
			case fint:
				exec = intExec(k)
			case fstr:
				exec = stringExec(k)
			default:
				panic("unknowing field type")
			}

			scols += fmt.Sprintf("%s,", exec)
		}

		scols = scols[:len(scols)-1]

		slms := sq.Select(scols).
			From(tbNm + " AS t1").
			Where(sq.Eq{fmt.Sprintf("CONVERT(VARCHAR(34), t1.%s_IDRRef, 2)", maintb): ref}).
			RunWith(mconn)

		mrows, err = slms.Query()

		if err != nil {
			panic(err)
		}

		// mrs
		mrs, err = readRows(mrows)

		version.VT[k] = make([]map[string]string, 0, len(mrs))
		for _, it := range mrs {
			version.VT[k] = append(version.VT[k], it)
		}
	}

	_, err = json.Marshal(version)
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

func refExec(fld string) string {
	return fmt.Sprintf("CONVERT(VARCHAR(34), t1.%s, 2) AS %s", fld, fld)
}

func boolExec(fld string) string {
	return fmt.Sprintf("CASE WHEN t1.%s = 0x01 THEN 'true' ELSE 'false' END AS %s", fld, fld)
}

func dateExec(fld string) string {
	return fmt.Sprintf("FORMAT(t1.%s, 'dd.MM.yyyy hh:mm:ss', 'ru-RU') as %s", fld, fld)
}

func stringExec(fld string) string {
	return fmt.Sprintf("t1.%s AS %s", fld, fld)
}

func intExec(fld string) string {
	return fmt.Sprintf("t1.%s AS %s", fld, fld)
}
