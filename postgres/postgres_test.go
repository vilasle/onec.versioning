package postgres

import (
	"fmt"
	"os"
	"testing"

	sq "github.com/Masterminds/squirrel"
)

func TestMain(t *testing.T) {

	entity := sq.Select("*").From("entity")
	sql, _, err := entity.ToSql()

	if err != nil {
		t.Fatal(err)
	}

	conf := PGAuth{
		User:     "onecversion",
		Password: "onecversion",
		Host:     "172.19.0.2",
		Port:     5432,
		SslMode:  false,
		Dbname:   "onecversion",
	}

	conn, err := NewConnection(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	rows, err := conn.Query(sql)
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var id, name, fullName, alias string
		if err := rows.Scan(&id, &name, &fullName, &alias); err == nil {
			fmt.Println(id, name, fullName, alias)
		}
	}
	rows.Close()
}

func TestLoadScheme(t *testing.T) {
	fd, err := os.Open("tables.json")
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	if err := LoadScheme(fd); err != nil {
		panic(err)
	}

}
