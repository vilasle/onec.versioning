package postgres

import(
	"database/sql"
	_ "github.com/lib/pq"
)

type PGConn struct {
	conn *sql.DB
	user string
	password string
	host string
	port int
	sslMode bool
}

func (c PGConn) String() string {
	return ""
}