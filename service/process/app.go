package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/vilamslep/iokafka"
	db "github.com/vilamslep/onec.versioning/dbms"
	mssql "github.com/vilamslep/onec.versioning/dbms/mssql"
	"github.com/vilamslep/onec.versioning/raw"
)

const DEBUG = true

type FakeTree map[string]map[string]string

var pgconn *sql.DB
var msconn *sql.DB

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

func main() {

	if DEBUG {
		dbg_LoadEnv()
	}

	//connection with PostgreSQL and MSSQL
	var err error
	if pgconn, msconn, err = CreateConnections(); err != nil {
		log.Fatalln(err)
	}

	defer msconn.Close()
	defer pgconn.Close()

	scanner := iokafka.NewScanner(loadKafkaConfigFromEnv(0))

	for scanner.Scan() {
		msg := scanner.Message()
		if err := handleMessage(msg.Value, msg.Key); err != nil {
			log.Println(err)
		}
	}

}

func handleMessage(content []byte, user []byte) error {
	var (
		tnum, ref string
		err       error
		version   = Version{VT: make(map[string][]map[string]string)}
	)

	rawstr := string(content)
	// expected raw has to be like {"#",7433d4e8-f07d-4ace-819f-00191a4913db,431:b3c1a4bf015829f711ed05ebb7aaf42c}
	if predata, ok := raw.CheckRawData(rawstr); ok {
		tnum, ref = raw.GetMainColumsAsVars(predata)
	}

	//setting of head fields
	rs, err := getFields(tnum, pgconn, false)
	if err != nil {
		panic(err)
	}

	if rs.Empty() {
		return fmt.Errorf("Not found main table settings")
	}
	it, _ := rs.First()
	maintb := it["table"]

	if data, err := getHeadOfVersion(rs, ref, msconn); err == nil {
		version.Main = data
	} else {
		return err
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
			return err
		}
	}
	//saving version in database

	record := VersionRecord{
		User:  string(user),
		Date: time.Now(),
		Version: version,
	}
	if err := record.Write(tnum, ref, pgconn); err != nil {
		return err
	}
	return err
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

func loadKafkaConfigFromEnv(offset int64) iokafka.ScannerConfig {

	host := os.Getenv("KAFKAHOST")
	port := os.Getenv("KAFKAPORT")
	topic := os.Getenv("KAFKATOPIC")
	group := os.Getenv("KAFKAGROUP")

	soc := fmt.Sprintf("%s:%s", host, port)

	return iokafka.ScannerConfig{
		Brokers:       []string{soc},
		Topic:         topic,
		GroupID:       group,
		AttemtsOnFail: 5,
		FailTimeout:   10,
		StartOffset:   offset,
	}
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
