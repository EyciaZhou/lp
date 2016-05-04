package main

import (
	"github.com/EyciaZhou/configparser"
	"github.com/EyciaZhou/picRouter/PicPipe"
	"os"
	"os/signal"
	"syscall"
	"time"
	"github.com/Sirupsen/logrus"
	"sync"
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

	QiniuAccessKey string `default:"fake"`
	QiniuSecretKey string `default:"fake"`
	QiniuBucket    string `default:"msghub-picture"`
}

func HandleSIGTERM(c chan os.Signal, whenDone func()) {
	_ = <-c
	whenDone()
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
		}
}

func handleError(errc <-chan error) {
	for err := range errc {
		logrus.Error(err.Error())
	}
}

func fanInTask(bufLen int, cs ...chan *pic.Task) chan *pic.Task {
	c := make(chan *pic.Task, bufLen)

	var wg sync.WaitGroup
	out := make(chan int)

	output := func(c <-chan int) {
		defer wg.Done()
		for n := range c {
			select {
			case out <- n:
			case <-done:
				return
			}
		}
	}

	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func buildNetwork(spc *pic.StorePipeCtx) {

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

	spc.BuildNetwork(10, )

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	go HandleSIGTERM(c, func(){ spc.Stop() })
}
