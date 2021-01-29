package main

type INDEX struct {
	INDEX_NAME  string
	TABLE_NAME  string
	INDEX_TYPE  string
	ISUNIQUE    string
	INDEX_COLS  string
	PARTITIONED string
}

func ModIndex(st, dt *TABLE) {
	om := make(map[string]INDEX)
	dm := make(map[string]INDEX)

	// 缺失列，列的属性（类型 长度 是否可为空）
	for _, v := range st.INDEXES {
		om[v.INDEX_NAME] = v
	}
	for _, v := range dt.INDEXES {
		dm[v.INDEX_NAME] = v
	}

	// 源端缺失的 删除
	for idx := range dm {
		//fmt.Println("-- 开始对比index", idx)

		if _, ok := om[idx]; !ok {
			// 不存在 删除
			dm[idx].dropindex()
		}
	}

	// 目标端缺失或者修改的

	for idx := range om {
		//fmt.Println("-- 开始对比index", idx)

		if _, ok := dm[idx]; !ok {
			// 不存在 创建
			om[idx].createindex()

		} else {
			// 存在 对比
			if om[idx].ISUNIQUE != dm[idx].ISUNIQUE || om[idx].INDEX_COLS != dm[idx].INDEX_COLS {

				om[idx].dropindex()
				om[idx].createindex()
			}
		}
	}
}

func (i INDEX) createindex() (str string) {

	switch i.INDEX_TYPE {
	case "BITMAP":
		str = str + "create bitmap"
	default:
		str = str + "create "
	}

	if i.ISUNIQUE == "UNIQUE" {
		str = str + " unique index "
	} else {
		str = str + " index "
	}

	str = str + i.INDEX_NAME + " on " + i.TABLE_NAME + " (" + i.INDEX_COLS + ")"

	if i.INDEX_TYPE == "NORMAL/REV" {
		str = str + "reverse"
	}

	if i.PARTITIONED == "YES" {
		str = str + " local"
	}
	str = str + ";\n"
	WriteSqlFile(Conf.SqlFile, str)
	return str
}

func (i INDEX) dropindex() (str string) {
	str = " drop index " + i.INDEX_NAME + ";\n"
	WriteSqlFile(Conf.SqlFile, str)
	return str
}
