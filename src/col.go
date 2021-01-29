package main

import (
	"database/sql"
	"fmt"
)

type COLUMN struct {
	COLUMN_NAME  string
	TABLE_NAME   string
	DATA_TYPE    string
	DATA_LENGTH  string
	NULLABLE     string
	DATA_DEFAULT sql.NullString
	COLUMN_ID    int
}

func (col COLUMN) DropCol() (str string) {
	str = "alter table drop column " + col.TABLE_NAME + ";"
	fmt.Println(str)
	WriteSqlFile(Conf.SqlFile, str)
	return str
}

func (col COLUMN) AddCol() (str string) {
	str = "alter table " + col.TABLE_NAME + " add " + col.COLUMN_NAME + " " + col.DATA_TYPE

	switch col.DATA_TYPE {
	case "VARCHAR2", "CHAR", "VARCHAR", "NVARCHAR2":
		str = str + "(" + col.DATA_LENGTH + ")"
	}
	if col.NULLABLE == "N" {
		str = str + " not null "
	}

	if col.DATA_DEFAULT.Valid {
		str = str + " default " + col.DATA_DEFAULT.String + "," + "\n"
	} else {
		str = str + "," + "\n"
	}

	str = str + ";\n"

	fmt.Println(str)
	WriteSqlFile(Conf.SqlFile, str)
	return str

}

// alter table DS_COUNTER_GROUP_NK_CS_L_5D modify dtchprbassnmeanul varchar2(20) default 1 not null;
func (col COLUMN) AlterCol() (str string) {
	str = "alter table " + col.TABLE_NAME + " modify " + col.COLUMN_NAME + " " + col.DATA_TYPE

	switch col.DATA_TYPE {
	case "VARCHAR2", "CHAR", "VARCHAR", "NVARCHAR2":
		str = str + "(" + col.DATA_LENGTH + ")"
	}
	if col.NULLABLE == "N" {
		str = str + " not null "
	}

	if col.DATA_DEFAULT.Valid {
		str = str + " default " + col.DATA_DEFAULT.String + "," + "\n"
	} else {
		str = str + "," + "\n"
	}

	str = str + ";\n"

	fmt.Println(str)
	WriteSqlFile(Conf.SqlFile, str)

	return str
}

func ModCol(st, dt *TABLE) {

	om := make(map[string]COLUMN)
	dm := make(map[string]COLUMN)

	// 缺失列，列的属性（类型 长度 是否可为空）
	for _, v := range st.COLS {
		om[v.COLUMN_NAME] = v
	}
	for _, v := range dt.COLS {
		dm[v.COLUMN_NAME] = v
	}

	// 需要先对比目标端存在，但是源端不存在的列
	for col := range dm {
		if _, ok := om[col]; !ok {
			// 源数据库不存在的列 删除
			dm[col].DropCol()
		}
	}

	// 再对比源端存在 目标端不存在的列 添加或者修改
	for col := range om {
		//fmt.Println("-- 开始对比列", col)

		if _, ok := dm[col]; !ok {
			// 目标数据库不存在 增加
			om[col].AddCol()

		} else {
			// 目标数据库存在则对比 类型 可否为空
			if om[col].DATA_TYPE != dm[col].DATA_TYPE || om[col].DATA_LENGTH != dm[col].DATA_LENGTH || om[col].NULLABLE != dm[col].NULLABLE {

				fmt.Println("Col DIF", om[col], "\nd=>", dm[col])
				om[col].AlterCol()

			}

		}

	}

}
