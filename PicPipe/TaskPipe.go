package pic
import (
	"database/sql"
	_"github.com/go-sql-driver/mysql"
	"fmt"
	"strconv"
	"github.com/nu7hatch/gouuid"
)

type PicTaskPipe interface {
	/*ErrorTask
		if errors when process the task, invoke this method
 	*/
	ErrorTask(task *Task) error

	/*FinishTask
		if task process finished, invoke this method
 	*/
	FinishTask(task *TaskFinished) error

	/*GetTasks
		fetch tasks
		if no available task, return nil, and no error.
	*/
	GetTasks(limit int)  ([]*Task, error)

	/*UpsertTask
		insert task to pipe
	 */
	UpsertTask(url string) (*Task, error)
}

type MySQLDialInfo struct {
	DBAddress  string
	DBPort     string
	DBName     string
	DBUsername string
	DBPassword string
}

func (p *MySQLDialInfo) Dial() (*sql.DB, error) {
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?timeout=20s",
		p.DBUsername, p.DBPassword, p.DBAddress, p.DBPort, p.DBName,
	)
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

type MySQLPicTaskPipe struct {
	db *sql.DB
}

func NewMySQLPicPipeUseConnectedDB(db *sql.DB) (*MySQLPicTaskPipe) {
	return &MySQLPicTaskPipe{
		db : db,
	}
}

func NewMySQLPicPipe(dinfo *MySQLDialInfo) (*MySQLPicTaskPipe, error) {
	db, err := dinfo.Dial()
	if err != nil {
		return nil, err
	}
	return NewMySQLPicPipeUseConnectedDB(db), nil
}

/*Task
	key is picture's id/name
*/
type Task struct {
	Key string
	URL string
}

type TaskFinished struct {
	Task

	NodeType string
	NodeName string
	MIMEInfo string
}

/*ErrorTask
if errors when process the task, invoke this method
 */
func (p *MySQLPicTaskPipe) ErrorTask(task *Task) error {
	_, err := p.db.Exec(`
	UPDATE pic_task_queue
        	SET status = 3
        	WHERE id = ?;`, task.Key)
	return err
}

/*FinishTask
if task process finished, invoke this method
 */
func (p *MySQLPicTaskPipe) FinishTask(task *TaskFinished) error {
	_, err := p.db.Exec(`
		UPDATE pic_task_queue
        		SET status = 2, nodetype = ?, nodenum = ?, ext = ?
        		WHERE id = ?;
	`, task.NodeType, task.NodeName, task.MIMEInfo, task.Key)
	return err
}

/*GetTasks
	fetch tasks
	if no available task, return nil, and no error.
	fetch limits in p.conf, owner set to p's uuid.
 */
func (p *MySQLPicTaskPipe) GetTasks(limit int) ([]*Task, error) {
	if limit <= 0 {
		return nil, nil
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	owner := uuid.String()

	//step 1, update sql, effect records but not return them, because can't finish this in one statement
	updateRst, err := p.db.Exec(`
		UPDATE pic_task_queue
			SET status = 1, owner = ?, time = CURRENT_TIMESTAMP, trytimes = trytimes + 1
			WHERE status = 0 AND owner = 0
			LIMIT ?;`, owner, limit)
	if err != nil {
		return nil, err
	}
	if n, _ := updateRst.RowsAffected(); n == 0 {	//because of this mysql implement won't return error here, so ignore error directly
		return nil, nil
	}

	//step 2, select effected records, and return them
	tasks, err := p.db.Query(
		`SELECT url, id from pic_task_queue
			WHERE status = 1 AND owner = ?;`, owner)
	if err != nil {
		return nil, err
	}
	defer tasks.Close()

	var (
		result = make([]*Task, limit)
		pic_url string
		pic_id string
		cnt = 0	//real count of records returned
	)

	for tasks.Next() {	//process records
		if err := tasks.Scan(&pic_url, &pic_id); err != nil {
			return nil, err
		}
		result[cnt] = &Task{Key: pic_id, URL: pic_url}
		cnt++
	}
	if err := tasks.Err(); err != nil {
		return nil, err
	}

	return result[:cnt], nil
}

func (p *MySQLPicTaskPipe) UpsertTask(url string) (*Task, error) {
	rowAffect, err := p.db.Exec(`
	INSERT INTO
			pic_task_queue (url, status, owner)
		SELECT
				?, 0, 0
			FROM DUAL
			WHERE NOT EXISTS (SELECT 1 FROM pic_task_queue WHERE url=?);
	`, url, url)
	if err != nil {
		return nil, err
	}

	var id int64

	if n, _ := rowAffect.RowsAffected(); n > 0 {	//new insert
		id, _ = rowAffect.LastInsertId()
	} else {	//already exists

		row := p.db.QueryRow(`
			SELECT id FROM pic_task_queue
				WHERE url=?
				LIMIT 1;
		`, url)

		err := row.Scan(&id)
		if err != nil {
			return nil, err
		}
	}

	return &Task{
		Key: strconv.FormatInt(id, 10),
		URL: url,
	}, nil
}