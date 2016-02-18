package main
/*
it make failed task in the MySQL reborn due to the failed times
 */

import (
	"github.com/zbindenren/logrus_mail"
	log "github.com/Sirupsen/logrus"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/EyciaZhou/configparser"
	"fmt"
	"time"
)

type Config struct {
	QueueTableName string `default:"pic_task_queue"`
	QueueTaskTimeout int64 `default:"1800"`

	DBAddress  string `default:"fake.com"`
	DBPort     string `default:"3306"`
	DBName     string `default:"msghub"`
	DBUsername string `default:"root"`
	DBPassword string `default:"123456"`

	ConnectTimeout int `default:"10"` //with second

	MailEnable	bool `default:"false"`
	MailApplicationName string `default:"Cleaner"`
	MailSMTPAddress     string `default:"127.0.0.1"`
	MailSMTPPort        int    `default:"25"`
	MailFrom            string `default:"fake@fake.com"`
	MailTo              string `default:"recv@fake.com"`

	MailUsername string `default:"nomailusername"`
	MailPassword string `default:"nomailpassword"`
}

var config Config

var (
	db     *sql.DB
	cleanStmt *sql.Stmt
)

func main() {
	configparser.AutoLoadConfig("cleaner", &config)

	if (config.MailEnable) {
		mailhook_auth, err := logrus_mail.NewMailAuthHook(config.MailApplicationName, config.MailSMTPAddress, config.MailSMTPPort, config.MailFrom, config.MailTo,
			config.MailUsername, config.MailPassword)

		if err == nil {
			log.AddHook(mailhook_auth)
			log.Error("Don't Worry, just for send a email to test")
		} else {
			log.Error("Can't Hook mail, ERROR:", err.Error())
		}
	}

	//generate sql session
	//root:123456@tcp(db.dianm.in:3306)/pictures
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?timeout=20s", config.DBUsername, config.DBPassword, config.DBAddress, config.DBPort, config.DBName)
	db, err := sql.Open("mysql", url)
	if err != nil {
		log.Error("Can't Connect DB REASON : " + err.Error())
		return
	}
	err = db.Ping()
	if err != nil {
		log.Error("Can't Connect DB REASON : " + err.Error())
		return
	}

	cleanStmt, err = db.Prepare(fmt.Sprintf(`
	UPDATE %s
		SET status = 0, time = NULL, owner = 0
		WHERE status = 3 OR (status = 1 AND trytimes < 5 AND UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(time) > %d);
	`, config.QueueTableName, config.QueueTaskTimeout))

	if err != nil {
		log.Error("Failed to Prepare1 ", err.Error())
		return
	}

	for {
		result, err := cleanStmt.Exec()
		if err != nil {
			log.Error("Failed clean : " + err.Error())
		} else {
			n, _ := result.RowsAffected()
			if n != 0 {
				log.Infof("Cleaned %d rows", n)
			}
		}

		time.Sleep(time.Second * time.Duration(config.QueueTaskTimeout))
	}
}
