package excel

import (
	"os"
	"encoding/csv"
)

type IExcel interface {
	GetColumnsOfExcel(fileName string) ([]string, error)
}

type Excel struct {

}

func (xl *Excel) GetColumnsOfExcel(fileName string) ([]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	return headers, nil
}