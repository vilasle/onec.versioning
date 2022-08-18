package main

import (
	"database/sql"
	"encoding/json"
)

type Version struct {
	Main map[string]string              `json:"Main"`
	VT   map[string][]map[string]string `json:"VT"`
}

func (v Version) Write(tabNum string, ref string, pgconn *sql.DB) error {
	content, err := json.Marshal(v)
	if err != nil {
		return err
	}

	act := PGActions{conn: pgconn, tableName: "version_" + tabNum }

	if err := act.createTable(); err == nil {
		err := act.addRowToVersionsRef()
		if err != nil {
			return err
		}
	} else {
		return err
	}
	lastId, err := act.getLastIdByRef(ref)
	if err != nil {
		return err
	}

	return act.addNewVersion(ref, string(content), lastId)
}
