package main

import (
	"database/sql"
	"fmt"
	"strings"
)

type TABLE struct {
	TABLE_NAME   string
	PARTITIONED  string // Y 分区表 N 普通表
	TEMPORARY    string // Y 临时表 N 非临时表
	PART_TYPE    string
	SUBPART_TYPE string
	PART_KEY     string
	SUBPART_KEY  sql.NullString
	HIGH_VALUE   string
	COLS         []COLUMN
	INDEXES      []INDEX
}

func DropTab(tab string) (str string) {
	str = "drop table " + tab + " purge;"
	WriteSqlFile(Conf.SqlFile, str)
	return str
}

func createTabs(db *sql.DB, tabs []string) {

	for _, v := range tabs {
		var str string
		fmt.Println("-- 创建缺失表", v)
		tab := NewTab(db, v)

		str = tab.gettabDDL()
		str = str + tab.getindexddl()
		WriteSqlFile(Conf.SqlFile, str)
	}
}

func (tab *TABLE) gettabDDL() string {

	var str string
	for _, v := range tab.COLS {

		str = str + v.COLUMN_NAME + " " + v.DATA_TYPE

		switch v.DATA_TYPE {
		case "VARCHAR2", "CHAR", "VARCHAR", "NVARCHAR2":
			str = str + "(" + v.DATA_LENGTH + ")"
		}

		if v.NULLABLE == "N" {
			str = str + " not null "
		}

		if v.DATA_DEFAULT.Valid {
			str = str + " default " + v.DATA_DEFAULT.String + "," + "\n"
		} else {
			str = str + "," + "\n"
		}
	}
	str = strings.TrimRight(str, ",\n")

	str = "create table " + tab.TABLE_NAME + "( \n" + str + "\n)"

	if tab.PARTITIONED == "YES" {
		v_parttime := tab.HIGH_VALUE[10:20]

		v_partition_name := "PART_" + strings.ReplaceAll(v_parttime, "-", "") + "00"

		str = str + "partition by range (" + tab.PART_KEY + ")\n"

		if tab.SUBPART_TYPE == "LIST" {
			str = str + "subpartition by list (" + tab.SUBPART_KEY.String + ")\n(partition" + v_partition_name +
				" values less than(to_date('" + v_parttime + "','yyyy-mm-dd'))\n(subpartition " + v_partition_name +
				"_C0 values(default)));"
		} else {
			str = str + "(partition " + v_partition_name + " values less than(to_date('" + v_parttime + "','yyyy-mm-dd'))\n" +
				"(subpartition " + v_partition_name + "_C0 values(default))\n)"
		}

	}
	str = str + ";\n"

	fmt.Println(str)
	return str

}

func (tab *TABLE) getindexddl() string {

	var str string
	for _, v := range tab.INDEXES {
		switch v.INDEX_TYPE {
		case "BITMAP":
			str = str + "create bitmap"
		default:
			str = str + "create "
		}

		if v.ISUNIQUE == "UNIQUE" {
			str = str + " unique index "
		} else {
			str = str + " index "
		}

		str = str + v.INDEX_NAME + " on " + tab.TABLE_NAME + " (" + v.INDEX_COLS + ")"

		if v.INDEX_TYPE == "NORMAL/REV" {
			str = str + "reverse"
		}

		if tab.PARTITIONED == "YES" {
			str = str + " local"
		}
		str = str + ";\n"
	}
	fmt.Println(str)
	return str

}

func NewTab(db *sql.DB, tab string) *TABLE {
	return GetOraTab(db, tab)
}
