package main

import (
	"fmt"
)

type VIEW struct {
	VIEW_NAME   string
	TEXT_LENGTH int
	TEXT        string
}

func NewView(db *DBModel, view string) *VIEW {
	return GetOraView(db, view)
}

func DropView(view string) (str string) {
	str = "drop view " + view + ";"
	WriteSqlFile(Conf.SqlFile, str)
	fmt.Println(str)
	return str
}

func createViews(db *DBModel, views []string) (str string) {

	for _, v := range views {
		vw := NewView(db, v)

		str = str + "create view " + vw.VIEW_NAME + " as\n" + vw.TEXT + ";\n/\n"
	}
	fmt.Println(str)
	WriteSqlFile(Conf.SqlFile, str)
	return str
}

func (vw *VIEW) AlterView() (str string) {
	str = "create or replace " + vw.VIEW_NAME + " as \n" + vw.TEXT + ";\n/\n"
	fmt.Println(str)
	WriteSqlFile(Conf.SqlFile, str)
	return str
}
