package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/vilamslep/iokafka"
	db "github.com/vilamslep/onec.versioning/dbms"
	mssql "github.com/vilamslep/onec.versioning/dbms/mssql"
	"github.com/vilamslep/onec.versioning/raw"
)

const DEBUG = true

type FakeTree map[string]map[string]string

func getFields(tableNumber string, conn *sql.DB, wantChild bool) (db.Result, error) {

	psql := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

	var tableExec string
	var joinExec string
	if wantChild {
		tableExec = "metadata_table_vt as t1"
		joinExec = "field as t2 on t1.id = t2.table_id_vt "
	} else {
		tableExec = "metadata_table_main as t1"
		joinExec = "field as t2 on t1.id = t2.table_id_main"
	}

	slt := psql.Select("t1.table_name as table, t2.field_name as field, t3.type_name as type").
		From(tableExec).
		LeftJoin(joinExec).
		LeftJoin("field_type as t3 on t2.id = t3.field_id").
		Where(sq.Eq{"t1.table_number": tableNumber}).
		RunWith(conn)

	if rows, err := slt.Query(); err == nil {
		return db.ReadRows(rows)
	} else {
		return nil, err
	}
}

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

func main() {

	var (
		pgconn, msconn *sql.DB
		tnum, ref      string
		err            error
		version        = Version{VT: make(map[string][]map[string]string)}
	)

	if DEBUG {
		dbg_LoadEnv()
	}

	rawstr := `{"#",7433d4e8-f07d-4ace-819f-00191a4913db,431:b3c1a4bf015829f711ed05ebb7aaf42c}`

	// expected raw has to be like {"#",7433d4e8-f07d-4ace-819f-00191a4913db,431:b3c1a4bf015829f711ed05ebb7aaf42c}
	if predata, ok := raw.CheckRawData(rawstr); ok {
		tnum, ref = raw.GetMainColumsAsVars(predata)
	}

	//connection with PostgreSQL and MSSQL
	if pgconn, msconn, err = CreateConnections(); err != nil {
		panic(err)
	}
	defer msconn.Close()
	defer pgconn.Close()

	//setting of head fields
	rs, err := getFields(tnum, pgconn, false)
	if err != nil {
		panic(err)
	}

	if rs.Empty() {
		panic("Not found main table settings")
	}
	it, _ := rs.First()
	maintb := it["table"]

	if data, err := getHeadOfVersion(rs, ref, msconn); err == nil {
		version.Main = data
	} else {
		panic(err)
	}
	//setting of value tables fields
	vtrs, err := getFields(tnum, pgconn, true)

	childTabs := transformResultToFakeTree(vtrs)

	for k, v := range childTabs {

		if mrs, err := getVTOfVersion(k, v, maintb, ref, msconn); err == nil {
			version.VT[k] = make([]map[string]string, 0, len(mrs))
			for _, it := range mrs {
				version.VT[k] = append(version.VT[k], it)
			}
		} else {
			panic(err)
		}
	}
	//saving version in database
	if err := version.Write(tnum, ref, pgconn); err != nil {
		panic(err)
	}
}

func transformResultToFakeTree(vtrs db.Result) FakeTree {
	childTabs := make(FakeTree)
	for _, vti := range vtrs {
		table := vti["table"]
		fld := vti["field"]
		tfld := vti["type"]
		if _, ok := childTabs[table]; !ok {
			childTabs[table] = make(map[string]string, 0)
		}
		childTabs[vti["table"]][fld] = tfld
	}

	return childTabs
}

func getHeadOfVersion(res db.Result, ref string, msconn *sql.DB) (map[string]string, error) {

	tbAlias := "t1"
	fld := make(map[string]string)
	item, err := res.First()
	if err != nil {
		return nil, err
	}
	maintb := item["table"]
	tbNm := "dbo." + maintb

	for _, fl := range res {
		fld[fl["field"]] = fl["type"]
	}

	var scols string
	for k, v := range fld {
		exec := mssql.SQLExec(k)
		scols += fmt.Sprintf("%s,", exec.Get(v, tbAlias))
	}

	scols = scols[:len(scols)-1]

	slms := sq.Select(scols).
		From(fmt.Sprintf("%s AS %s", tbNm, tbAlias)).
		Where(sq.Eq{"CONVERT(VARCHAR(34), t1._IDRRef, 2)": ref}).
		RunWith(msconn)

	mrows, err := slms.Query()

	if err != nil {
		return nil, err
	}

	mrs, err := db.ReadRows(mrows)

	if mrs.Empty() || len(mrs) > 1 {
		return nil, err
	}
	return mrs.First()
}

func getVTOfVersion(key string, value map[string]string,
	mainTableName string, ref string, msconn *sql.DB) (db.Result, error) {

	fields := make(map[string]string)
	tbNm := "dbo." + key

	for flk, flv := range value {
		fields[flk] = flv
	}

	var scols string
	for k, v := range fields {
		exec := mssql.SQLExec(k)
		scols += fmt.Sprintf("%s,", exec.Get(v, "t1"))
	}

	scols = scols[:len(scols)-1]

	slms := sq.Select(scols).
		From(tbNm + " AS t1").
		Where(sq.Eq{fmt.Sprintf("CONVERT(VARCHAR(34), t1.%s_IDRRef, 2)", mainTableName): ref}).
		RunWith(msconn)

	mrows, err := slms.Query()

	if err != nil {
		return nil, err
	}

	return db.ReadRows(mrows)

}

func dbg_LoadEnv() error {
	f, err := os.Open("dev.env")
	if err != nil {
		return err
	}

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		path := strings.Split(sc.Text(), "=")
		if len(path) < 2 {
			continue
		}
		os.Setenv(path[0], path[1])
	}
	return sc.Err()
}
