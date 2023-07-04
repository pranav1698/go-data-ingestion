package database

import (
	"fmt"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pranav1698/go-data-ingestion/env"
	"github.com/pranav1698/go-data-ingestion/record"
)

type IDatabase interface {
	ConnectDatabase() (*sql.DB, error)
	GetColumnsOfDatabase() ([]string, error)
	InsertInSitesTable(*sql.DB, string) (int, error)
	InsertInMetricsTable(*sql.DB, record.MetricRecord) (error)
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

func (db *Database) GetColumnsOfDatabase() ([]string, error) {
	dataBase, err := db.ConnectDatabase()
	if err != nil {
		return nil, err
	}
	defer dataBase.Close()

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

func (db *Database) InsertInSitesTable(dataBase *sql.DB, targetPage string) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM sites WHERE TargetPage = '%s'", targetPage)
	var count int
	err := dataBase.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, err
	}

	var id int
	if count > 0 {
		selectQuery := fmt.Sprintf("SELECT TargetPageId FROM sites WHERE TargetPage = '%s'", targetPage)
		err = dataBase.QueryRow(selectQuery).Scan(&id)
		if err != nil {
			return 0, err
		}
	} else {
		insertQuery := fmt.Sprintf("INSERT INTO sites (TargetPage) VALUES ('%s')", targetPage)
		rows, err := dataBase.Query(insertQuery)
		if err != nil {
			return 0, err
		}
		defer rows.Close()

		selectQuery := fmt.Sprintf("SELECT TargetPageId FROM sites WHERE TargetPage = '%s'", targetPage)
		err = dataBase.QueryRow(selectQuery).Scan(&id)
		if err != nil {
			return 0, err
		}
	}

	return id, nil
}

func (db *Database) InsertInMetricsTable(dataBase *sql.DB, metricRecord record.MetricRecord) (error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM metrics WHERE TargetPageId = '%d' AND Date = '%s'", metricRecord.TargetPageId, metricRecord.Date)
	var count int
	err := dataBase.QueryRow(query).Scan(&count)
	if err != nil {
		return err
	}
	
	if count > 0 {
		updateQuery := fmt.Sprintf("UPDATE metrics SET `Incoming links` = '%s' AND `Linking sites` = '%s' WHERE TargetPageId = '%d' AND Date = '%s'", metricRecord.IncomingLinks, metricRecord.LinkingSites, metricRecord.TargetPageId, metricRecord.Date)
		rows, err := dataBase.Query(updateQuery)
		if err != nil {
			return err
		}
		rows.Close()
	} else {
		insertQuery := fmt.Sprintf("INSERT INTO metrics (TargetPageId, Date, `Incoming links`, `Linking sites`) VALUES ('%d', '%s', '%s', '%s')", metricRecord.TargetPageId, metricRecord.Date, metricRecord.IncomingLinks, metricRecord.LinkingSites)
		rows, err := dataBase.Query(insertQuery)
		if err != nil {
			return err
		}
		rows.Close()
	}

	return nil
}