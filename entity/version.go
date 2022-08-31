package entity

import (
	"database/sql"
	"encoding/json"
	"time"

	pg "github.com/vilamslep/onec.versioning/dbms/postgres"
)

type VersionRecord struct {
	User string
	Date time.Time
	Version
}

type Version struct {
	Main map[string]string              `json:"Main"`
	VT   map[string][]map[string]string `json:"VT"`
}

func (v VersionRecord) Write(tabNum string, ref string, pgconn *sql.DB) error {
	content, err := json.Marshal(v)
	if err != nil {
		return err
	}

	act := pg.NewOperator(pgconn, ("version_" + tabNum))

	if err := act.CreateVersionsTable(); err == nil {
		err := act.AddRowToVersionsRef()
		if err != nil {
			return err
		}
	} else {
		return err
	}
	lastId, err := act.GetLastIdByRef(ref)
	if err != nil {
		return err
	}

	return act.AddNewVersion(ref, string(content), lastId, v.Date, v.User)
}
