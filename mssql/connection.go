package mssql

import(
	"fmt"
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"

)

type MSSQLAuth struct {
	User string
	Password string
	Host string
	Port int
	Dbname string
}

func (c MSSQLAuth) String() string {
	return fmt.Sprintf("server=%s;port=%d;user id=%s;password=%s;database=%s", 
		c.Host, c.Port, c.User, c.Password, c.Dbname)
}

func NewConnection(conf MSSQLAuth) (*sql.DB, error) {

	if db, err := sql.Open("mssql", conf.String()); err == nil {
		if err := db.Ping(); err != nil {
			return nil, err
		}
		return db, nil
	} else {
		return nil, err
	}
}