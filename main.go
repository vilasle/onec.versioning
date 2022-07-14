package main

import (
	// 	"database/sql"
	// 	"fmt"
	"os"

	pg "github.com/vilamslep/onec.versioning/postgres"
	// 	_ "github.com/lib/pq"
	// 	"github.com/vilamslep/onec.versioning/logger"
)

// type ConnectionConfig struct {
// 	User     string
// 	Password string
// 	Host     string
// 	Port     int
// 	Database string
// 	SSlMode  bool
// }

// func (c ConnectionConfig) String() string {
// 	var mode string
// 	if c.SSlMode {
// 		mode = "enable"
// 	} else {
// 		mode = "disable"
// 	}
// 	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=%s", c.Host, c.User, c.Password, c.Database, mode)
// }

func main() {
	fd, err := os.Open("base.json")
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	if err := pg.LoadScheme(fd); err != nil {
		panic(err)
	}

	// setting.Run()
	// logger.Infof("simple zap logger example")
	// pgConf := ConnectionConfig{
	// 	Host:     "172.18.0.2",
	// 	Port:     5432,
	// 	User:     "onecvers",
	// 	Password: "142543",
	// 	Database: "onecvers",
	// 	SSlMode:  false,
	// }

	// db, err := createConnection(pgConf)
	// if err != nil {
	// 	panic(err)
	// }
	// defer db.Close()

	// txt := `select * from test`

	// rows, err := db.Query(txt)
	// if err != nil {
	// 	panic(err)
	// }
	// for rows.Next() {
	// 	var id, f1, f2 string
	// 	err := rows.Scan(&id, &f1, &f2)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	} else {
	// 		fmt.Println(id, f1, f2)
	// 	}

	// }
}

// func createConnection(pgConf ConnectionConfig) (*sql.DB, error) {
// 	db, err := sql.Open("postgres", pgConf.String())
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer func() {
// 		if r := recover(); r != nil {
// 			logger.Error() //"Panic. Recovered in f", r
// 			fmt.Println("Panic. Recovered in f", r)
// 		}
// 	}()

// 	if err := db.Ping(); err != nil {
// 		return nil, err
// 	}
// 	return db, nil
// }
