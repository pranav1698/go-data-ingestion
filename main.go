package main

import (
	"os"
	"log"
	"fmt"
	"errors"
	
	"github.com/pranav1698/go-data-ingestion/fileUtil"
	"github.com/pranav1698/go-data-ingestion/database"
	"github.com/pranav1698/go-data-ingestion/excel"
	"github.com/pranav1698/go-data-ingestion/record"
)

func main() {
	log.Print("Starting Application....")

	fileName := "/home/pranav/go/src/go-data-ingestion/files/https___www.thisisbarry.com_-Top target pages-2022-08-16.csv"
	file, err := os.Open(fileName)
	if err != nil {
		log.Println("Error opening file: %s", err)
		return
	}
	defer file.Close()

	err = CheckFile(fileName)
	if err != nil {
		log.Println("Error: ", err)
		return 
	}

	date := GetDateFromFileName(fileName)
	log.Println(date)

	err = CheckColumnsInDatabase(fileName)
	if err != nil {
		log.Println("Error:", err)
		return
	}

	err = InsertRecordInDatabase(fileName, date)
	if err != nil {
		log.Println("Error:", err)
		return
	}
}

func CheckFile(fileName string) (error) {
	var util fileUtil.IFileUtil = &fileUtil.FileUtil{}
	isExcel := util.CheckExtension(fileName)
	if !isExcel {
		err := errors.New("Not a Excel File, please provide an excel or csv file as input")
		return err
	}

	checkFormat := util.CheckFormat(fileName)
	if !checkFormat {
		err := errors.New("Please check that file adheres to predefined format, for e.g.: https___www.thisisbarry.com_-Top target pages-2022-08-01.csv")
		return err
	}

	return nil
}

func GetDateFromFileName(fileName string) (string) {
	var util fileUtil.IFileUtil = &fileUtil.FileUtil{}
	
	date := util.GetDate(fileName)
	return date
}

func CheckColumnsInDatabase(fileName string) (error) {
	var db database.IDatabase = &database.Database{}
	dataBase, err := db.ConnectDatabase()
	if err != nil {
		return err
	}
	defer dataBase.Close()

	dbColumns, err := db.GetColumnsOfDatabase(dataBase)
	if err != nil {
		return err
	}
	
	var xl excel.IExcel = &excel.Excel{}
	excelColumnHeaders, err := xl.GetColumnsOfExcel(fileName)
	if err != nil {
		return err
	}

	for _, columnHeader := range excelColumnHeaders {
		if columnHeader == "Target page" {
			continue
		}
		flag := false

		for _, dbColumnHeader := range dbColumns {
			if dbColumnHeader == columnHeader {
				flag = true
			}
		}
		
		if !flag {
			err := fmt.Errorf("%s not present in database.", columnHeader)
			return err
		}
		
	}

	return nil
}

func InsertRecordInDatabase(fileName string, date string) (error) {
	var xl excel.IExcel = &excel.Excel{}
	records, err := xl.GetRowsOfExcel(fileName)
	if err != nil {
		return err
	}

	var db database.IDatabase = &database.Database{}
	dataBase, err := db.ConnectDatabase()
	if err != nil {
		return err
	}

	columnMap := map[string]int{}
	for index, header := range records[0] {
		columnMap[header] = index
	}
	
	records = records[1:]
	for _, row := range records {
		targetPageId, err := db.InsertInSitesTable(dataBase, row[0])
		if err != nil {
			return err
		}

		metricRecord := record.MetricRecord{
			TargetPageId: targetPageId,
			Date: date,
			IncomingLinks: row[columnMap["Incoming links"]],
			LinkingSites: row[columnMap["Linking sites"]],
		}
		
		err = db.InsertInMetricsTable(dataBase, metricRecord)
		if err != nil {
			return err
		}
	}

	dataBase.Close()
	return nil
}