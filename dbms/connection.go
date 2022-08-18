package dbms

import (
	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/lib/pq"
)

type ConfigConnection interface {
	String() string
	Driver() string
	NewConnection()(*sql.DB, error)
}

type BasicAuth struct {
	User     string
	Password string
}

type Socket struct {
	Host string
	Port int
}

func NewConnection(c ConfigConnection) (*sql.DB, error) {
	if db, err := sql.Open(c.Driver(), c.String()); err == nil {
		if err := db.Ping(); err != nil {
			return nil, err
		}
		return db, nil
	} else {
		return nil, err
	}
}