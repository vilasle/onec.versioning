package lib

import (
	"database/sql"

	"github.com/pkg/errors"
	db "github.com/vilamslep/onec.versioning/db"
	pg "github.com/vilamslep/onec.versioning/db/postgres"
)

type Records []Record

type Record struct {
	Id            string `json:"id"`
	OnecTimestamp string `json:"date"`
	User          string `json:"user"`
	Number        string `json:"number"`
}

type Version struct {
	Id      string `json:"id"`
	Ref     string `json:"ref"`
	Number  string `json:"number"`
	User    string `json:"user"`
	Content string `json:"content"`
}

var mainErr error = errors.New("can not create connections with databases")

func CreateDatabaseConnection() (err error) {

	var pgerr error

	pg.Session, pgerr = createDBConnection(pg.GetConfigFromEnv)

	if pgerr != nil {
		err = mainErr

		if pgerr != nil {
			err = errors.Wrap(err, pgerr.Error())
		}
	}

	return err
}

func createDBConnection(gettingConf func() (db.ConfigConnection, error)) (*sql.DB, error) {
	conf, err := gettingConf()
	if err != nil {
		return nil, err
	}
	return conf.NewConnection()
}

func ExecQuery(q string, args ...any) error {
	_, err := pg.Session.Exec(q, args...)
	return err
}

func GetLastIdByRef(ref string) (int, error) {
	q := txtLastIdByRef()
	var plug, lastId int
	if err := pg.Session.QueryRow(q, ref).Scan(&plug, &lastId); err == nil {
		return lastId, nil
	} else {
		return 0, err
	}
}

func txtLastIdByRef() string {
	return `SELECT t1.plug as plug,
	CASE WHEN t2.version_number IS NULL THEN 0 ELSE t2.version_number END AS version_number
	FROM (SELECT 1 AS plug) AS t1 
	LEFT JOIN (
		SELECT version_number AS version_number FROM public.versions AS t
		WHERE ref = $1 
		ORDER BY version_number DESC LIMIT 1) AS t2
	ON t2.version_number > 0;`
}

func ListVersions(ref string) (Records, error) {

	q := `SELECT id, version_user, to_char(created_at,'YYYYMMDDHH24MISS'), version_number FROM public.versions AS t WHERE t.ref = $1`

	rows, err := pg.Session.Query(q, ref)

	if err != nil {
		return nil, err
	}

	data := make(Records, 0)
	for rows.Next() {
		var id, user, timestamp, number string

		if err := rows.Scan(&id, &user, &timestamp, &number); err != nil {
			return nil, err
		}
		data = append(data, Record{
			OnecTimestamp: timestamp,
			User:          user,
			Number:        number,
			Id:            id,
		})
	}

	return data, nil
}

func GetVersionById(id string) ([]byte, error) {
	q := `SELECT content FROM versions WHERE id = $1`
	row := pg.Session.QueryRow(q, id)

	var content []byte
	if err := row.Scan(&content); err == nil {
		return content, nil
	} else {
		return nil, err
	}
}
