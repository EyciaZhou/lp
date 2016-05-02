package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/EyciaZhou/configparser"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/Sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"runtime"
	"github.com/EyciaZhou/picRouter/readsizer"
)

//------------------------------------------Download Machine-----------------------------------------
var (
	path = ""
)

func writeToDisk(rc io.ReadCloser, fn string) error {
	file, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer rc.Close()
	defer file.Close()

	buf := make([]byte, 32*1024)

	_, err = io.CopyBuffer(file, rc, buf)

	if err != nil {
		return err
	}
	return nil
}

var (
	db     *sql.DB
	client *http.Client
)

/*ResponseToReadSizer:
	convert http.Response.Body to a ReadSizer, won't invoke Close

	if Response including ContentLength in HEAD, will use that number as size,
	read all bytes of body to measure the size and store these bytes to buff for read otherwise

	to avoiding the infinite body, limit body size to 10MB here
 */
func ResponseToReadSizer(resp *http.Response) (readsizer.ReadSizer, error) {
	r := readsizer.NewSizeLimitReader(resp.Body, 100*readsizer.MB)
	if resp.ContentLength < 0 {
		return readsizer.NewSizeReader(r)
	}
	return readsizer.NewSizeReaderWithSize(r, resp.ContentLength)
}

func GetPictureReturnsReader(url string) (io.ReadCloser, string, error) {
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Add("User-Agent", `Mozilla/5.0 (Linux; Android 4.3; Nexus 7 Build/JSS15Q) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2307.2 Safari/537.36`)

	var resp *http.Response

	for try := 0; try < 5; try++ {
		resp, err = client.Do(req)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, "", err
	}

	content_type := resp.Header.Get("Content-Type")

	flag := false

	extName := ""

	for i, typ := range config.TypeAcceptable {
		//why use hasPrefix is because of some website returns something like "image/jpg; charset=UTF-8"
		//and that's not illegal
		if strings.HasPrefix(content_type, typ) {
			//if content_type == typ {
			flag = true
			extName = config.TypeExtName[i]
			break
		}
	}
	if !flag {
		resp.Body.Close()
		return nil, "", errors.New("image content-type not match, the content-type is" + content_type)
	}

	return resp.Body, extName, nil
}

type Config struct {
	RootPath string `default:"/data/pic"`

	NodeID int `default:"0"`

	ThreadNumber          int `default:"5"`
	ThreadStartDelay      int `default:"1"`  //with second
	ThreadNoTaskSleepTime int `default:"20"` //with second
	NumberOfEachFetchTask int `default:"5"`

	QueueTableName string `default:"pic_task_queue"`

	DBAddress  string `default:"fake.com"`
	DBPort     string `default:"3306"`
	DBName     string `default:"msghub"`
	DBUsername string `default:"root"`
	DBPassword string `default:"123456"`

	TypeAcceptable []string `default:"[\"image/png\", \"image/jpeg\", \"image/gif\"]"`
	TypeExtName    []string `default:"[\"png\", \"jpg\", \"gif\"]"`

	ConnectTimeout int `default:"10"` //with second
}

var config Config

func loadConfig() {
	var err error

	//load
	configparser.AutoLoadConfig("downloader", &config)
	configparser.ToJson(&config)

	if config.RootPath[len(config.RootPath)-1] != '/' {
		config.RootPath += "/"
	}

	path = config.RootPath

	err = os.MkdirAll(path, 0777)
	if err != nil {
		panic(err)
	}
	log.Info("Load config end")
}

var (
	updateStmt *sql.Stmt
	selectStmt *sql.Stmt
	finishStmt *sql.Stmt
	errorStmt  *sql.Stmt
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	loadConfig()

	//progress connect client
	client = &http.Client{
		Timeout: time.Duration(time.Second * (time.Duration)(config.ConnectTimeout)),
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

	updateStmt, err = db.Prepare(fmt.Sprintf(`
	UPDATE %s
		SET status = 1, owner = ?, time = CURRENT_TIMESTAMP, trytimes = trytimes + 1
		WHERE status = 0 AND owner = 0
		LIMIT %d;
	`, config.QueueTableName, config.NumberOfEachFetchTask))

	if err != nil {
		log.Error("Failed to Prepare1 ", err.Error())
		return
	}

	selectStmt, err = db.Prepare(fmt.Sprintf(`
	SELECT url, id from %s
		WHERE status = 1 AND owner = ?;
	`, config.QueueTableName))

	if err != nil {
		log.Error("Failed to Prepare2 ", err.Error())
		return
	}

	finishStmt, err = db.Prepare(fmt.Sprintf(`
    UPDATE %s
        SET status = 2, nodenum = ?, ext = ?
        WHERE id = ?;
	`, config.QueueTableName))

	if err != nil {
		log.Error("Failed to Prepare3 ", err.Error())
		return
	}

	errorStmt, err = db.Prepare(fmt.Sprintf(`
    UPDATE %s
        SET status = 3
        WHERE id = ?;
	`, config.QueueTableName))

	if err != nil {
		log.Error("Failed to Prepare4 ", err.Error())
		return
	}

	//start download task
	for taskNo := 0; taskNo < config.ThreadNumber-1; taskNo++ {
		go downloadAndStoreTask(config.NodeID*100 + taskNo, config.NodeID)
		time.Sleep(time.Second * (time.Duration)(config.ThreadStartDelay))
	}
	if config.ThreadNumber > 0 {
		downloadAndStoreTask(config.NodeID*100 + config.ThreadNumber - 1, config.NodeID)
	}
}
