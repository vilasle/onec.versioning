package postgres

import(
	"fmt"
	"database/sql"
	_ "github.com/lib/pq"
)

type PGAuth struct {
	User string
	Password string
	Host string
	Port int
	SslMode bool
	Dbname string
}


func (c PGAuth) String() string {
	var mode string 
	if c.SslMode {
		mode = "enable"
	} else {
		mode = "disable"
	}
	return fmt.Sprintf("user=%s password=%s dbname=%s sslmode=%s host=%s port=%d", 
		c.User, c.Password, c.Dbname, mode, c.Host, c.Port)
}

func NewConnection(conf PGAuth) (*sql.DB, error) {
	db, err := sql.Open("postgres", conf.String())
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}
	
	return db, nil
}