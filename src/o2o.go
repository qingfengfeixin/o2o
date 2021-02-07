package main

import (
	"fmt"
	"strconv"
)

type o2o struct {
	db *DB
}

func O2O() {

	o := &o2o{
		db: NewDB(Conf),
	}

	defer o.db.Close()

	//NewTab(db.Sb,"PARA_CELL_L")

	// 1 表
	o.diftab()

	// 2 视图
	o.difview()

	// 3 序列
	o.difseq()

	// 4 存储过程和函数
	o.difpro()

}

func (o *o2o) diftab() {

	sts := GetOraTabs(&o.db.Sb)
	dts := GetOraTabs(&o.db.Db)

	exceptdrop := Except(dts, sts)
	exceptadd := Except(sts, dts)
	inter := Intersect(sts, dts)

	// 删除多余表
	for _, v := range exceptdrop {
		DropTab(v)
	}

	//创建缺失表
	createTabs(&o.db.Sb, exceptadd)

	count :=len(inter)

	// 已存在的表对比表结构和索引
	for i, v := range inter {
		fmt.Println("-- 开始对比表：", strconv.Itoa(i+1)+"/"+strconv.Itoa(count), v)

		st := NewTab(&o.db.Sb, v)
		dt := NewTab(&o.db.Db, v)

		// 缺失列，列的属性（类型 长度 是否可为空）
		ModCol(st, dt)

		//修改变更索引  增加索引 修改索引
		ModIndex(st, dt)
	}

}

func (o *o2o) difview() {
	svs := GetOraViews(&o.db.Sb)
	dvs := GetOraViews(&o.db.Db)

	exceptdrop := Except(dvs, svs)
	exceptadd := Except(svs, dvs)
	inter := Intersect(svs, dvs)

	// 删除多余视图
	for _, v := range exceptdrop {
		DropView(v)
	}

	// 增加缺失视图
	createViews(&o.db.Sb, exceptadd)

	// 对比已经存在的视图
	for i, v := range inter {
		fmt.Println("-- 开始对比视图：", i, v)

		st := NewView(&o.db.Sb, v)
		dt := NewView(&o.db.Db, v)

		// 如果text_length 不一样则重新创建view
		if st.TEXT_LENGTH != dt.TEXT_LENGTH {
			st.AlterView()
		}

	}

}

func (o *o2o) difseq() {
	sss := GetOraSeq(&o.db.Sb)
	dss := GetOraSeq(&o.db.Db)

	exceptdrop := Except(dss, sss)
	exceptadd := Except(sss, dss)

	// 删除多余
	for _, v := range exceptdrop {
		dropSeq(v)
	}
	// 增加缺失
	createSeqs(exceptadd)
}

func (o *o2o) difpro() {
	sps := GetOraPros(&o.db.Sb)
	dps := GetOraPros(&o.db.Db)

	exceptdrop := Except(dps, sps)
	exceptadd := Except(sps, dps)
	inter := Intersect(sps, dps)

	// 删除多余

	for _, v := range exceptdrop {
		dropPro(v)
	}
	// 增加缺失
	createPros(&o.db.Sb, exceptadd)

	// todo 修改存在
	for i, v := range inter {
		fmt.Println("-- 开始对比存储过程：", i, v)

	}

}
