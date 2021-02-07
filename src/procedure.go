package main

import (
	"fmt"
)

type PROCEDURE struct {
	PROCEDURE_NAME string
	PROCEDURE_TYPE string
	text           string
}

func NewPro(db *DBModel, Pro string) *PROCEDURE {
	return GetOraPro(db, Pro)
}

func dropPro(pro string) (str string) {
	str = "drop procedure " + pro + ";\n"
	WriteSqlFile(Conf.SqlFile, str)
	fmt.Println(str)
	return str
}

func createPros(db *DBModel, pros []string) (str string) {

	for _, v := range pros {
		pro := NewPro(db, v)

		str = str + "create " + pro.text + ";\n/\n"
	}
	fmt.Println(str)
	WriteSqlFile(Conf.SqlFile, str)
	return str
}
