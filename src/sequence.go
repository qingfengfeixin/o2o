package main

import "fmt"

type sequence struct {
	sequence_name string
}

func dropSeq(seq string) (str string) {
	str = "drop sequence " + seq + ";"
	WriteSqlFile(Conf.SqlFile, str)
	fmt.Println(str)
	return str
}

func createSeqs(seqs []string) (str string) {

	for _, v := range seqs {
		str = str + "create sequence " + v + ";\n"
	}
	fmt.Println(str)
	WriteSqlFile(Conf.SqlFile, str)
	return str
}
