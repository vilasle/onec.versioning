package postgres

import (
	"fmt"
	"time"
)

type PGActions struct {
	tableName string
}

func NewOperator(tableName string) *PGActions {
	return &PGActions{tableName: tableName}
}

func (act *PGActions) CreateVersionsTable() error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id serial PRIMARY KEY, ref VARCHAR(32) NOT NULL, keywords TEXT,content TEXT NOT NULL, 
		user_id VARCHAR(36) NOT NULL, version_timestamp TIMESTAMP NOT NULL, version_number INT NOT NULL);`, act.tableName)

	_, err := Session.Exec(q)
 	return err
}

func (act *PGActions) AddRowToVersionsRef() error {
	q := fmt.Sprintf(`
	INSERT INTO version_ref (metadata_id, table_name)
		SELECT * FROM ( SELECT t2.id AS metadata_id, '%s' AS table_name
		FROM metadata_table_main AS t1
		LEFT JOIN metadata AS t2 ON t1.metadata_id = t2.id
		WHERE t1.table_number = 431
	) AS tmp 
	WHERE NOT EXISTS (SELECT table_name FROM version_ref WHERE table_name = '%s' LIMIT 1);`,
		act.tableName, act.tableName)

	_, err := Session.Exec(q)
	return err
}

func (act *PGActions) GetLastIdByRef(ref string) (int, error) {
	q :=
		fmt.Sprintf(`SELECT t1.plug as plug,
		CASE WHEN t2.version_number IS NULL THEN 0 ELSE version_number END AS version_number
		FROM (SELECT 1 AS plug) AS t1 LEFT JOIN (
			SELECT version_number AS version_number FROM %s AS t 
			WHERE ref = $1 ORDER BY version_number DESC LIMIT 1) AS t2 
			ON t2.version_number > 0;`,
			act.tableName)

	var plug, lastId int
	if err := Session.QueryRow(q, ref).Scan(&plug, &lastId); err == nil {
		return lastId, nil
	} else {
		return 0, err
	}
}

func (act *PGActions) AddNewVersion(ref string, content string, lastId int, timestamp time.Time, user string) error {
	q := fmt.Sprintf(`INSERT INTO %s (ref, content, version_number, version_timestamp, user_id) VALUES($1, $2, $3, $4, $5);`, act.tableName)

	_, err := Session.Exec(q, ref, string(content), (lastId + 1), timestamp, user)

	return err
}