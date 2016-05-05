package main

import (
	"github.com/EyciaZhou/configparser"
	"github.com/EyciaZhou/picRouter/PicPipe"
	"github.com/Sirupsen/logrus"
	"os"
	"os/signal"
	"time"
)

type Config struct {
	DBAddress  string `default:"127.0.0.1"`
	DBPort     string `default:"3306"`
	DBName     string `default:"msghub"`
	DBUsername string `default:"root"`
	DBPassword string `default:"fmttm233"`

	TaskFetchLimit                    int `default:"10"`
	SleepDurationWhenFetchErrorOrNull int `default:"60"`
	HttpConnectionTryTimes            int `default:"5"`
	HttpTimeout                       int `default:"60"`

	BufLen_tasks          int `default:"5"`
	BufLen_fetched        int `default:"5"`
	BufLen_errc           int `default:"5"`
	BufLen_finishQueue    int `default:"5"`
	BufLen_errorTaskQueue int `default:"2"`

	Cnt_StateFinishTask int `default:"10"`
	Cnt_StateErrorTask  int `default:"2"`
	Cnt_StateGetTask    int `default:"10"`
	Cnt_StateStore      int `default:"10"`
	Cnt_StateFetch      int `default:"10"`

	QiniuAccessKey string `default:"fake"`
	QiniuSecretKey string `default:"fake"`
	QiniuBucket    string `default:"msghub-picture"`
}

func loadConf() (*pic.MySQLDialInfo, *pic.QiniuStorerConf, *pic.StorePipeCtxConfig) {
	var Conf Config
	configparser.AutoLoadConfig("downloader", &Conf)

	return &pic.MySQLDialInfo{
			Conf.DBAddress, Conf.DBPort, Conf.DBName,
			Conf.DBUsername, Conf.DBPassword,
		},
		&pic.QiniuStorerConf{
			Conf.QiniuAccessKey,
			Conf.QiniuSecretKey,
			Conf.QiniuBucket,
		},
		&pic.StorePipeCtxConfig{
			Conf.TaskFetchLimit,
			(time.Duration)(Conf.SleepDurationWhenFetchErrorOrNull) * time.Second,
			Conf.HttpConnectionTryTimes,
			(time.Duration)(Conf.HttpTimeout) * time.Second,

			Conf.BufLen_tasks, Conf.BufLen_fetched,	Conf.BufLen_errc,
			Conf.BufLen_finishQueue, Conf.BufLen_errorTaskQueue,

			Conf.Cnt_StateFinishTask, Conf.Cnt_StateErrorTask, Conf.Cnt_StateGetTask,
			Conf.Cnt_StateStore, Conf.Cnt_StateFetch,
		}
}

func main() {
	confMySql, confQiniu, confSPC := loadConf()

	picTaskPipe, err := pic.NewMySQLPicPipe(confMySql)
	if err != nil {
		panic(err)
	}

	storer := pic.NewQiniuStorer(confQiniu)
	if err != nil {
		panic(err)
	}

	spc := pic.NewStorePipeCtx(confSPC, picTaskPipe, storer)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		_ = <- c
		spc.Stop()
	}()

	spc.SetErrorProcessor(func(err error) { logrus.Error(err.Error()) })

	spc.BuildNetwork()

	spc.Loop()
}
