package lib

import (
	"database/sql"

	"github.com/pkg/errors"
	db "github.com/vilamslep/onec.versioning/dbms"
	pg "github.com/vilamslep/onec.versioning/dbms/postgres"
)

var mainErr error = errors.New("can not create connections with databases")

func CreateConnections() (err error) {

	var pgerr error

	pg.Session, pgerr = createDBConnection(pg.GetConfigFromEnv)


	if pgerr != nil {
		err = mainErr

		if pgerr != nil {
			err = errors.Wrap(err, pgerr.Error())
		}
	}

	return err
}

func createDBConnection(gettingConf func() (db.ConfigConnection, error)) (*sql.DB, error) {
	conf, err := gettingConf()
	if err != nil {
		return nil, err
	}
	return conf.NewConnection()
}


