package main

import (
	"database/sql"
	"fmt"
	_ "github.com/sijms/go-ora"
	"regexp"
	"strings"
)

type DB struct {
	Sb *sql.DB
	Db *sql.DB
}

func NewDB(c *Config) *DB {
	d := &DB{
		Sb: Conn(c.Sc.Driver, c.Sc.Dsn),
		Db: Conn(c.Dc.Driver, c.Dc.Dsn),
	}
	return d
}

func Conn(driver, dsn string) *sql.DB {

	db, err := sql.Open(driver, dsn)
	if err != nil {
		fmt.Println("connect err =", err)
		return nil
	}
	err = db.Ping()
	if err != nil {
		fmt.Println("ping err =", err)
		return nil
	}
	return db
}

func (d *DB) Close() {

	if d.Sb != nil {
		d.Sb.Close()
	}
	if d.Db != nil {
		d.Db.Close()
	}
}

// get all ora tables
func GetOraTabs(db *sql.DB) (tabs []string) {

	stmt, err := db.Prepare("select table_name from user_tables")
	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
	}

	rows, err := stmt.Query()
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
	}

	for rows.Next() {
		var tab string
		err = rows.Scan(&tab)
		if err != nil {
			fmt.Println("scan err =", err)
		}
		tabs = append(tabs, tab)
	}
	return tabs
}

func GetOraTab(db *sql.DB, tab string) *TABLE {

	tabOBJ := &TABLE{TABLE_NAME: tab}

	stmt, err := db.Prepare("SELECT PARTITIONED, TEMPORARY FROM USER_TABLES " +
		" where table_name = :1")
	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
	}

	row := stmt.QueryRow(tab)
	err = row.Scan(&tabOBJ.PARTITIONED, &tabOBJ.TEMPORARY)
	if err != nil {
		fmt.Println(err)
	}

	if tabOBJ.PARTITIONED == "YES" {
		getPartTab(db, tabOBJ)
	}

	getTabCol(db, tabOBJ)
	getTabIndex(db, tabOBJ)

	return tabOBJ

}

func getPartTab(db *sql.DB, t *TABLE) {

	stmt, err := db.Prepare("SELECT /*+ rule */ " +
		"A.PARTITIONING_TYPE AS PART_TYPE,A.SUBPARTITIONING_TYPE AS SUBPART_TYPE," +
		"B.COLUMN_NAME  AS PART_KEY,C.COLUMN_NAME AS SUBPART_KEY,D.HIGH_VALUE " +
		"FROM USER_PART_TABLES A,USER_PART_KEY_COLUMNS B,USER_SUBPART_KEY_COLUMNS C, USER_TAB_PARTITIONS D " +
		"WHERE A.TABLE_NAME = B.NAME " +
		"AND A.TABLE_NAME = C.NAME(+) " +
		"AND A.PARTITIONING_TYPE = 'RANGE' " +
		"AND A.SUBPARTITIONING_TYPE IN ('NONE', 'LIST') " +
		"AND A.TABLE_NAME NOT LIKE 'BIN$%' " +
		"AND A.TABLE_NAME = D.TABLE_NAME " +
		"AND D.PARTITION_POSITION = 1  " +
		"AND A.TABLE_NAME =:1")

	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
	}
	row := stmt.QueryRow(t.TABLE_NAME)
	err = row.Scan(&t.PART_TYPE, &t.SUBPART_TYPE, &t.PART_KEY, &t.SUBPART_KEY, &t.HIGH_VALUE)
	if err != nil {
		fmt.Println(err)
	}

}

func getTabCol(db *sql.DB, t *TABLE) {
	col := &COLUMN{}

	stmt, err := db.Prepare("SELECT COLUMN_NAME, table_name,DATA_TYPE,DATA_LENGTH," +
		"NULLABLE,DATA_DEFAULT ,COLUMN_ID FROM USER_TAB_COLUMNS where table_name = :1 order by COLUMN_ID")
	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
	}

	rows, err := stmt.Query(t.TABLE_NAME)
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
	}

	for rows.Next() {
		err = rows.Scan(&col.COLUMN_NAME,
			&col.TABLE_NAME,
			&col.DATA_TYPE,
			&col.DATA_LENGTH,
			&col.NULLABLE,
			&col.DATA_DEFAULT,
			&col.COLUMN_ID,
		)
		if err != nil {
			fmt.Println("scan err =", err)
		}
		t.COLS = append(t.COLS, *col)
	}

}

func getTabIndex(db *sql.DB, t *TABLE) {

	stmt, err := db.Prepare("SELECT A.INDEX_NAME,A.table_name,A.INDEX_TYPE,A.UNIQUENESS,A.PARTITIONED," +
		"LISTAGG(B.COLUMN_NAME, ',') WITHIN GROUP(ORDER BY B.COLUMN_POSITION) AS COLUMN_NAME " +
		"FROM USER_INDEXES A, USER_IND_COLUMNS B " +
		"WHERE A.INDEX_NAME = B.INDEX_NAME " +
		"AND A.TABLE_NAME = :1 " +
		"AND B.TABLE_NAME = :2 " +
		"GROUP BY A.TABLE_NAME,A.INDEX_NAME, A.INDEX_TYPE,A.UNIQUENESS,A.PARTITIONED ")

	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := stmt.Query(t.TABLE_NAME, t.TABLE_NAME)
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

	for rows.Next() {
		index := &INDEX{}
		err = rows.Scan(&index.INDEX_NAME,
			&index.TABLE_NAME,
			&index.INDEX_TYPE,
			&index.ISUNIQUE,
			&index.PARTITIONED,
			&index.INDEX_COLS,
		)
		if err != nil {
			fmt.Println("scan err =", err)
			return
		}

		if index.INDEX_TYPE == "FUNCTION-BASED NORMAL" {
			getFunIndexCol(db, index)
		}

		t.INDEXES = append(t.INDEXES, *index)
	}

}

func getFunIndexCol(db *sql.DB, i *INDEX) {

	stmt, err := db.Prepare("SELECT  column_expression from user_ind_expressions where index_name= :1")

	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
	}
	row := stmt.QueryRow(i.INDEX_NAME)
	var index_cols_fun string

	err = row.Scan(&index_cols_fun)
	if err != nil {
		fmt.Println(err)
	}
	index_cols_fun = strings.ReplaceAll(index_cols_fun, "\"", "")

	reg := regexp.MustCompile(`SYS[\w.]*\$`)
	i.INDEX_COLS = reg.ReplaceAllString(i.INDEX_COLS, index_cols_fun)

}

func GetOraViews(db *sql.DB) (views []string) {
	stmt, err := db.Prepare("select view_name from user_views order by 1")
	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
	}

	rows, err := stmt.Query()
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
	}

	for rows.Next() {
		var view string
		err = rows.Scan(&view)
		if err != nil {
			fmt.Println("scan err =", err)
		}
		views = append(views, view)

	}

	return views
}

func GetOraView(db *sql.DB, view string) *VIEW {

	vw := &VIEW{VIEW_NAME: view}

	stmt, err := db.Prepare("SELECT text_length, text FROM user_views " +
		" where view_name = :1")
	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
	}

	row := stmt.QueryRow(view)
	err = row.Scan(&vw.TEXT_LENGTH, &vw.TEXT)
	if err != nil {
		fmt.Println(err)
	}
	return vw

}

func GetOraPro(db *sql.DB, P string) *PROCEDURE {

	pro := &PROCEDURE{PROCEDURE_NAME: P}

	stmt, err := db.Prepare("SELECT type, text FROM USER_SOURCE " +
		" where name = :1 order by line")
	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
	}

	rows, err := stmt.Query(P)
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
	}

	for rows.Next() {
		var text string
		err = rows.Scan(&pro.PROCEDURE_TYPE,
			&text,
		)
		if err != nil {
			fmt.Println("scan err =", err)
		}
		pro.text = pro.text + text
	}
	return pro

}

func GetOraPros(db *sql.DB) (pros []string) {
	stmt, err := db.Prepare("select object_name from user_procedures where object_type='PROCEDURE' order by 1")
	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
	}

	rows, err := stmt.Query()
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
	}

	for rows.Next() {
		var pro string
		err = rows.Scan(&pro)
		if err != nil {
			fmt.Println("scan err =", err)
		}
		pros = append(pros, pro)

	}
	return pros
}

func GetOraSeq(db *sql.DB) (seqs []string) {
	stmt, err := db.Prepare("select sequence_name from USER_SEQUENCES order by 1")
	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
	}

	rows, err := stmt.Query()
	defer rows.Close()
	if err != nil {
		fmt.Println(err)
	}

	for rows.Next() {
		var seq string
		err = rows.Scan(&seq)
		if err != nil {
			fmt.Println("scan err =", err)
		}
		seqs = append(seqs, seq)

	}
	return seqs
}
