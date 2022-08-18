package mssql

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"

	db "github.com/vilamslep/onec.versioning/dbms"
)

type Config struct {
	db.BasicAuth
	db.Socket
	Dbname string
}

func (c Config) String() string {
	return fmt.Sprintf("server=%s;port=%d;user id=%s;password=%s;database=%s",
		c.Host, c.Port, c.User, c.Password, c.Dbname)
}

func (c Config) Driver() string {
	return "mssql"
}

func (c Config) NewConnection()(*sql.DB, error) {
	return db.NewConnection(c)
} 


func GetConfigFromEnv() (db.ConfigConnection, error) {
	config := Config{}
	config.Host = os.Getenv("MSSQLHOST")
	strport := os.Getenv("MSSQLPORT")

	if port, err := strconv.Atoi(strport); err == nil {
		config.Port = port
	} else {
		return config, err
	}

	config.User = os.Getenv("MSSQLUSER")
	config.Password = os.Getenv("MSSQLPASSWORD")
	config.Dbname = os.Getenv("MSSQLDATABASE")

	return config, nil
}
