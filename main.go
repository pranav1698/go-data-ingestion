package main

import (
	"fmt"
	"log"
	"database/sql"
	
	_ "github.com/go-sql-driver/mysql"
	"github.com/pranav1698/go-data-ingestion/env"
	"github.com/gin-gonic/gin"
)

func main() {
	log.Print("Starting Application....")
	
	r := gin.Default()
	


	conf := env.NewConfig("pranav", "pranavsql", "3306", "data_ingestion")

	dbUrl := fmt.Sprintf("%s:%s@tcp(127.0.0.1:%s)/%s", conf.DbUsername, conf.DbPassword, conf.DbSqlPort, conf.Database)
	
	db, err := sql.Open("mysql", dbUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}