package main

import (
	"database/sql"

	"github.com/pkg/errors"
	db "github.com/vilamslep/onec.versioning/dbms"
	mssql "github.com/vilamslep/onec.versioning/dbms/mssql"
	pg "github.com/vilamslep/onec.versioning/dbms/postgres"
)

var mainErr error = errors.New("can not create connections with databases")

func CreateConnections() (*sql.DB, *sql.DB, error) {
	var (
		err         error
		areThereErr bool
	)

	pgconn, pgerr := createDBConnection(pg.GetConfigFromEnv)

	msconn, mserr := createDBConnection(mssql.GetConfigFromEnv)

	areThereErr = pgerr != nil || mserr != nil

	if areThereErr {
		err = mainErr

		if pgerr != nil {
			err = errors.Wrap(err, pgerr.Error())
		}

		if mserr != nil {
			err = errors.Wrap(err, mserr.Error())
		}
	}

	return pgconn, msconn, err
}

func createDBConnection(gettingConf func() (db.ConfigConnection, error)) (*sql.DB, error) {
	conf, err := gettingConf()
	if err != nil {
		return nil, err
	}
	return conf.NewConnection()
}



