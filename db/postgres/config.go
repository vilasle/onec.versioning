package postgres

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"github.com/Masterminds/squirrel"
	db "github.com/vilamslep/onec.versioning/db"
)

var Session *sql.DB

type Config struct {
	db.BasicAuth
	db.Socket	
	SslMode bool
	Dbname string
}


func (c Config) String() string {
	var mode string 
	if c.SslMode {
		mode = "enable"
	} else {
		mode = "disable"
	}
	return fmt.Sprintf("user=%s password=%s dbname=%s sslmode=%s host=%s port=%d", 
		c.User, c.Password, c.Dbname, mode, c.Host, c.Port)
}

func (c Config) Driver() string {
	return "postgres"
}

func (c Config) NewConnection()(*sql.DB, error) {
	return db.NewConnection(c)
} 

func GetConfigFromEnv() (db.ConfigConnection, error) {
	config := Config{}
	config.Host = os.Getenv("PGHOST")
	strport := os.Getenv("PGPORT")
	
	if port, err := strconv.Atoi(strport); err == nil {
		config.Port = port
	} else {
		return config, err
	}

	config.User = os.Getenv("PGUSER")
	config.Password = os.Getenv("PGPASSWORD")
	config.Dbname = os.Getenv("PGDBNAME")
	 
	return config, nil
}

func AddConnectionToBuilder(builder squirrel.SelectBuilder) squirrel.SelectBuilder {
	return builder.RunWith(Session)
}