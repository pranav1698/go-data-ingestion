package database

import (
	"fmt"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pranav1698/go-data-ingestion/env"
)

type IDatabase interface {
	ConnectDatabase() (*sql.DB, error)
	GetColumnsOfDatabase(*sql.DB) ([]string, error)
}

type Database struct {

}

func (db *Database) ConnectDatabase() (*sql.DB, error) {
	conf := env.NewConfig("pranav", "pranavsql", "3306", "data_ingestion")

	dbUrl := fmt.Sprintf("%s:%s@tcp(127.0.0.1:%s)/%s", conf.DbUsername, conf.DbPassword, conf.DbSqlPort, conf.Database)

	dataBase, err := sql.Open("mysql", dbUrl)
	if err != nil {
		return nil, err
	}
	
	return dataBase, nil
}

func (db *Database) GetColumnsOfDatabase(dataBase *sql.DB) ([]string, error) {
	rows, err := dataBase.Query("SELECT * FROM metrics LIMIT 1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	return columns, nil
}