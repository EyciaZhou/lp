package pic

import (
	"errors"
	"fmt"
	"github.com/EyciaZhou/picRouter/readsizer"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type countWG struct {
	sync.WaitGroup
	Count int32 // Race conditions, only for info logging.
}

func (cg *countWG) Add(delta int) {
	atomic.AddInt32(&cg.Count, (int32)(delta))
	cg.WaitGroup.Add(delta)
}

func (cg *countWG) Done() {
	atomic.AddInt32(&cg.Count, -1)
	cg.WaitGroup.Done()
}

/*responseToReadCloseSizer:
convert http.Response.Body to a ReadSizer

read all bytes from body to measure the size and store these bytes to buff for reading

to avoiding the infinite body, limit body size to 40MB here
*/
func responseToReadSizer(resp *http.Response) (readsizer.ReadSizer, error) {
	return readsizer.ReadCloserToReadSizer(resp.Body, 40*readsizer.MB)
}

type StorePipeCtxConfig struct {
	Conf_TaskFetchLimit                    int
	Conf_SleepDurationWhenFetchErrorOrNull time.Duration
	Conf_HttpConnectionTryTimes            int
	Conf_HttpTimeout                       time.Duration

	BufLen_tasks, BufLen_fetched, BufLen_errc int
	BufLen_finishQueue, BufLen_errorTaskQueue int

	Cnt_StateFinishTask, Cnt_StateErrorTask, Cnt_StateGetTask int
	Cnt_StateStore, Cnt_StateFetch                            int
}

type StorePipeCtx struct {
	StorePipeCtxConfig

	done chan struct{}
	finished chan struct{}
	cleanOnce sync.Once

	httpClient http.Client

	taskPipe PicTaskPipe
	storer   Storer

	tasks          chan *Task
	fetched        chan taskWithReadSizer
	errc           chan error
	finishQueue    chan *TaskFinished
	errorTaskQueue chan *Task

	errorProcessor func(error)

	state_FinishTaskWG   countWG
	state_ErrorTaskWG    countWG
	state_StoreWG        countWG
	state_FetchWG        countWG
	state_GetTaskWG      countWG
	state_ProcessErrorWG countWG
}

func (p *StorePipeCtx) BuildNetwork() {
	p.state_ProcessErrorWG.Add(1)
	go p.State_ProcessError()

	p.state_ErrorTaskWG.Add(p.Cnt_StateErrorTask)
	for i := 0; i < p.Cnt_StateErrorTask; i++ {
		go p.State_ErrorTask()
	}

	p.state_FinishTaskWG.Add(p.Cnt_StateFinishTask)
	for i := 0; i < p.Cnt_StateFinishTask; i++ {
		go p.State_FinishTask()
	}

	p.state_StoreWG.Add(p.Cnt_StateStore)
	for i := 0; i < p.Cnt_StateStore; i++ {
		go p.State_Store()
	}

	p.state_FetchWG.Add(p.Cnt_StateFetch)
	for i := 0; i < p.Cnt_StateFetch; i++ {
		go p.State_Fetch()
	}

	p.state_GetTaskWG.Add(p.Cnt_StateGetTask)
	for i := 0; i < p.Cnt_StateGetTask; i++ {
		go p.State_GetTask()
	}
}

func NewStorePipeCtx(conf *StorePipeCtxConfig, picTaskPipe PicTaskPipe, storer Storer) *StorePipeCtx {
	return &StorePipeCtx{
		*conf,
		make(chan struct{}),
		make(chan struct{}),
		sync.Once{},
		http.Client{Timeout: conf.Conf_HttpTimeout},
		picTaskPipe,
		storer,

		make(chan *Task, conf.BufLen_tasks),
		make(chan taskWithReadSizer, conf.BufLen_fetched),
		make(chan error, conf.BufLen_errc),
		make(chan *TaskFinished, conf.BufLen_finishQueue),
		make(chan *Task, conf.BufLen_errorTaskQueue),

		func(error) {},

		countWG{}, countWG{}, countWG{},
		countWG{}, countWG{}, countWG{},
	}
}

func (p *StorePipeCtx) State_ProcessError() {
	defer p.state_ProcessErrorWG.Done()

	for err := range p.errc {
		if p.errorProcessor != nil {
			p.errorProcessor(err)
		}
	}
}

func (p *StorePipeCtx) SetErrorProcessor(f func(error)) {
	p.errorProcessor = f
}

func (p *StorePipeCtx) clean() {
	go func() {
		for {
			select {
			case <-p.finished:
				return
			case <-time.After(time.Second):
			}

			fmt.Printf(`
state_GetTaskWG:%d
state_FetchWG:%d
state_StoreWG:%d
state_FinishTaskWG:%d
state_ErrorTaskWG:%d
state_ProcessErrorWG:%d
			`,
				p.state_GetTaskWG.Count,
				p.state_FetchWG.Count,
				p.state_StoreWG.Count,
				p.state_FinishTaskWG.Count,
				p.state_ErrorTaskWG.Count,
				p.state_ProcessErrorWG.Count,
			)
		}
	}()


	p.state_GetTaskWG.Wait()
	close(p.tasks)

	p.state_FetchWG.Wait()
	close(p.fetched)

	p.state_StoreWG.Wait()
	close(p.finishQueue)
	close(p.errorTaskQueue)

	p.state_FinishTaskWG.Wait()
	p.state_ErrorTaskWG.Wait()

	close(p.errc)
	p.state_ProcessErrorWG.Wait()

	close(p.finished)
}

func (p *StorePipeCtx) Loop() {
	<-p.done

	p.cleanOnce.Do(p.clean)

	<-p.finished
}

func (p *StorePipeCtx) Stop() {
	close(p.done)
}

func (p *StorePipeCtx) State_FinishTask() {
	defer p.state_FinishTaskWG.Done()

	for task := range p.finishQueue {
		err := p.taskPipe.FinishTask(task)
		if err != nil {
			select {
			case p.errc <- err:
			case <-p.done:
				return
			}
		}
	}

}

func (p *StorePipeCtx) State_ErrorTask() {
	defer p.state_ErrorTaskWG.Done()

	for task := range p.errorTaskQueue {
		err := p.taskPipe.ErrorTask(task)
		if err != nil {
			select {
			case p.errc <- err:
			case <-p.done:
				return
			}
		}
	}
}

func (p *StorePipeCtx) State_GetTask() {
	defer p.state_GetTaskWG.Done()

	for {
		tasks, err := p.taskPipe.GetTasks(p.Conf_TaskFetchLimit)
		if err != nil || tasks == nil || len(tasks) == 0 {
			select {
			case p.errc <- err:
			case <-p.done:
				return
			}

			select {
			case <-time.After(p.Conf_SleepDurationWhenFetchErrorOrNull):
			case <-p.done:
				return
			}
			continue
		}
		for _, task := range tasks {
			select {
			case p.tasks <- task:
			case <-p.done:
				return
			}
		}
	}
}

const (
	_USER_AGENT = `Mozilla/5.0 (Linux; Android 4.3; Nexus 7 Build/JSS15Q) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2307.2 Safari/537.36`
)

func (p *StorePipeCtx) getResp(task *Task) (resp *http.Response, e error) {
	req, err := http.NewRequest("GET", task.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("User-Agent", _USER_AGENT)

	for try := 0; try < p.Conf_HttpConnectionTryTimes; try++ {
		resp, err = p.httpClient.Do(req)
		if err == nil {
			break
		}
	}
	return
}

var (
	acceptContentType = []string{
		"application/x-jpe", "image/jpg", "image/jpeg",
		"image/png", "application/x-png",
		"image/gif",
	}
	contentTypeWashed = []string{
		"image/jpeg", "image/jpeg", "image/jpeg",
		"image/png", "image/png",
		"image/gif",
	}
)

func (p *StorePipeCtx) judgeContentType(ContentType string) string {
	if ContentType == "" {
		return ""
	}

	for i, typ := range acceptContentType {
		if strings.HasPrefix(ContentType, typ) {
			return contentTypeWashed[i]
		}
	}

	return ""
}

type taskWithReadSizer struct {
	Task           *Task
	RS             readsizer.ReadSizer
	WashedMIMEType string
}

func (p *StorePipeCtx) State_Store() {
	defer p.state_StoreWG.Done()

	for t := range p.fetched {
		err := p.storer.Store(t.RS, t.Task.Key)
		if err != nil {
			if p.handleError(t.Task, err) {
				return
			}
			continue
		}

		select {
		case <-p.done:
			return
		case p.finishQueue <- &TaskFinished{
			*t.Task, p.storer.StorerType(), p.storer.StorerKey(), t.WashedMIMEType,
		}:
		}
	}
}

func (p *StorePipeCtx) handleError(task *Task, err error) bool {
	select {
	case p.errorTaskQueue <- task:
	case <-p.done:
		return true
	}

	select {
	case p.errc <- err:
	case <-p.done:
		return true
	}

	return false
}

func (p *StorePipeCtx) State_Fetch() {
	defer p.state_FetchWG.Done()

	for task := range p.tasks {
		resp, err := p.getResp(task)
		if err != nil {
			if p.handleError(task, err) {
				return
			}
			continue
		}

		washedMIMEType := p.judgeContentType(resp.Header.Get("Content-Type"))
		if washedMIMEType == "" {
			if p.handleError(task, errors.New("not allowed Content-Type")) {
				return
			}
			continue
		}

		rs, err := responseToReadSizer(resp)
		if err != nil {
			if p.handleError(task, err) {
				return
			}
			continue
		}

		select {
		case <-p.done:
			return
		case p.fetched <- taskWithReadSizer{task, rs, washedMIMEType}:
		}
	}
}
