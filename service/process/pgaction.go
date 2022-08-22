package main

import (
	"database/sql"
	"fmt"
	"time"
)

type PGActions struct {
	conn      *sql.DB
	tableName string
}

func (act *PGActions) createTable() error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id serial PRIMARY KEY, ref VARCHAR(32) NOT NULL, keywords TEXT, 
		content TEXT NOT NULL, version_timestamp TIMESTAMP NOT NULL, version_number INT NOT NULL);`, act.tableName)

	_, err := act.conn.Exec(q)
	return err
}

func (act *PGActions) addRowToVersionsRef() error {
	q := fmt.Sprintf(`
	INSERT INTO version_ref (metadata_id, table_name)
		SELECT * FROM ( SELECT t2.id AS metadata_id, '%s' AS table_name
		FROM metadata_table_main AS t1
		LEFT JOIN metadata AS t2 ON t1.metadata_id = t2.id
		WHERE t1.table_number = 431
	) AS tmp 
	WHERE NOT EXISTS (SELECT table_name FROM version_ref WHERE table_name = '%s' LIMIT 1);`,
		act.tableName, act.tableName)

	_, err := act.conn.Exec(q)
	return err
}

func (act *PGActions) getLastIdByRef(ref string) (int, error) {
	q :=
		fmt.Sprintf(`SELECT t1.plug as plug,
		CASE WHEN t2.version_number IS NULL THEN 0 ELSE version_number END AS version_number
		FROM (SELECT 1 AS plug) AS t1 LEFT JOIN (
			SELECT version_number AS version_number FROM %s AS t 
			WHERE ref = $1 ORDER BY version_number DESC LIMIT 1) AS t2 
			ON t2.version_number > 0;`,
			act.tableName)

	var plug, lastId int
	if err := act.conn.QueryRow(q, ref).Scan(&plug, &lastId); err == nil {
		return lastId, nil
	} else {
		return 0, err
	}
}

func (act *PGActions) addNewVersion(ref string, content string, lastId int, timestamp time.Time) error {
	q := fmt.Sprintf(`INSERT INTO %s (ref, content, version_number, version_timestamp) VALUES($1, $2, $3, $4);`, act.tableName)

	_, err := act.conn.Exec(q, ref, string(content), (lastId + 1), timestamp)

	return err
}
