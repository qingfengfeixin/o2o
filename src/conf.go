package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"os"
)

var (
	confPath string
	Conf     = &Config{}
)

type Config struct {
	Sc      dbstr `toml:"sou"`
	Dc      dbstr `toml:"dest"`
	SqlFile string
}

type dbstr struct {
	Driver string
	Dsn    string
}

func init() {
	flag.StringVar(&confPath, "conf", "../conf/conf.toml", "default config path")

}

func Init() {
	if _, err := toml.DecodeFile(confPath, &Conf); err != nil {
		panic(err)
	}

	// sqlfile文件 存在则清空 不存在则创建
	f, err := os.OpenFile(Conf.SqlFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()

}
