package main

import (
	"bufio"
	"log"
	"os"
)

// 补集
func Except(a []string, b []string) (s1 []string) {

	m1 := make(map[string]interface{})
	m2 := make(map[string]interface{})

	for i, v := range a {
		m1[v] = i
	}

	for i, v := range b {
		m2[v] = i
	}

	for i := range m1 {
		if _, ok := m2[i]; !ok {
			// 不存在
			s1 = append(s1, i)
		} else {
			// fmt.Println("存在：",i)
		}
	}
	return s1

}

//交集
func Intersect(a []string, b []string) (s1 []string) {

	m1 := make(map[string]interface{})
	m2 := make(map[string]interface{})

	for i, v := range a {
		m1[v] = i
	}

	for i, v := range b {
		m2[v] = i
	}

	for i := range m1 {
		if _, ok := m2[i]; !ok {
			// 不存在
		} else {
			// fmt.Println("存在：",i)
			s1 = append(s1, i)
		}
	}
	return s1

}



func WriteSqlFile(SqlFile,str string) {
	fileHandle, err := os.OpenFile(SqlFile, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("open file error :", err)
		return
	}
	defer fileHandle.Close()
	// NewWriter 默认缓冲区大小是 4096
	// 需要使用自定义缓冲区的writer 使用 NewWriterSize()方法
	buf := bufio.NewWriter(fileHandle)
	// 字符串写入
	buf.WriteString(str)
	// 将缓冲中的数据写入
	err = buf.Flush()
	if err != nil {
		log.Println("flush error :", err)
	}
}
