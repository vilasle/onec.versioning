package postgres

import (
	"os"
	"testing"

	"github.com/vilamslep/onec.versioning/dbms"
)

func TestLoadScheme(t *testing.T) {
	fd, err := os.Open("tables.json")
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		BasicAuth: dbms.BasicAuth{
			User:     "onecversion",
			Password: "onecversion",
		},
		Socket: dbms.Socket{
			Host: "172.16.100.2",
			Port: 5432,
		},
		Dbname:  "onecversion",
		SslMode: false,
	}
	if err := LoadScheme(fd, config); err != nil {
		t.Fatal(err)
	}
	fd.Close()
}

func TestLoadEnums(t *testing.T) {
	fd, err := os.Open("enum.json")
	if err != nil {
		t.Fatal(err)
	}
	config := Config{
		BasicAuth: dbms.BasicAuth{
			User:     "onecversion",
			Password: "onecversion",
		},
		Socket: dbms.Socket{
			Host: "172.16.100.2",
			Port: 5432,
		},
		Dbname:  "onecversion",
		SslMode: false,
	}
	if err := LoadEnums(fd, config); err != nil {
		t.Fatal(err)
	}
}
