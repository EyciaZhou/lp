package pic
import (
	"github.com/EyciaZhou/picRouter/readsizer"
	"net/http"
	"time"
	"strings"
	"errors"
)

/*responseToReadCloseSizer:
convert http.Response.Body to a ReadCloseSizer

read all bytes from body to measure the size and store these bytes to buff for reading

to avoiding the infinite body, limit body size to 40MB here
*/
func responseToReadCloseSizer(resp *http.Response) (readsizer.ReadSizer, error) {
	return readsizer.ReadCloserToReadSizer(resp.Body, 40*readsizer.MB)
}

type StorePipeCtxConfig struct {
	Conf_TaskFetchLimit                    int
	Conf_SleepDurationWhenFetchErrorOrNull time.Duration
	Conf_HttpConnectionTryTimes            int
	Conf_HttpTimeout                       time.Duration
}

type StorePipeCtx struct {
	StorePipeCtxConfig

	done chan struct{}

	httpClient http.Client

	taskPipe PicTaskPipe
	storer   Storer
}

func NewStorePipeCtx(conf *StorePipeCtxConfig, picTaskPipe PicTaskPipe, storer Storer) *StorePipeCtx {
	return &StorePipeCtx{
		*conf,
		make(chan struct{}),
		http.Client{
			Timeout:conf.Conf_HttpTimeout,
		},
		picTaskPipe,
		storer,
	}
}

func (p *StorePipeCtx) Stop() {
	close(p.done)
}

func (p *StorePipeCtx) State_FinishTask(input <-chan *TaskFinished) (errc chan<- error)  {
	errc = make(chan error)
	defer close(errc)

	go func() {
		for task := range input {
			err := p.taskPipe.FinishTask(task)
			if err != nil {
				select {
				case errc <- err:
				case <-p.done:
					return
				}
			}
		}
	}()

	return
}

func (p *StorePipeCtx) State_ErrorTask(input <-chan *Task) (errc chan<- error) {
	errc = make(chan error)
	defer close(errc)

	go func() {
		for task := range input {
			err := p.taskPipe.ErrorTask(task)
			if err != nil {
				select {
				case errc <- err:
				case <-p.done:
					return
				}
			}
		}
	}()

	return
}

func (p *StorePipeCtx) State_GetTask() (out chan<- *Task, errc chan<- error) {
	out = make(chan *Task)
	errc = make(chan error)
	defer close(out)
	defer close(errc)

	go func() {
		for {
			tasks, err := p.taskPipe.GetTasks(p.Conf_TaskFetchLimit)
			if err != nil || tasks == nil || len(tasks) == 0 {
				select {
				case errc <- err:
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
				case out <- task:
				case <-p.done:
					return
				}
			}
		}
	}()

	return
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

func (p *StorePipeCtx) State_Store(input <-chan *Task) (finish chan<- *TaskFinished, rollback chan<- *Task, errc chan<- error) {
	handleError := func(task *Task, err error) bool {
		select {
		case rollback <- task:
		case <-p.done:
			return true
		}

		select {
		case errc <- err:
		case <-p.done:
			return true
		}

		return false
	}

	finish = make(chan *TaskFinished)
	rollback = make(chan *Task)
	errc = make(chan error)

	defer close(finish)
	defer close(rollback)
	defer close(errc)

	go func() {
		for task := range input {
			resp, err := p.getResp(task)
			if err != nil {
				if handleError(task, err) {
					return
				}
				continue
			}

			washedMIMEType := p.judgeContentType(resp.Header.Get("Content-Type"))
			if washedMIMEType == "" {
				if handleError(task, errors.New("not allowed Content-Type")) {
					return
				}
				continue
			}

			rcs, err := responseToReadCloseSizer(resp)
			if err != nil {
				if handleError(task, err) {
					return
				}
				continue
			}

			err = p.storer.Store(rcs, task.Key)
			if err != nil {
				if handleError(task, err) {
					return
				}
				continue
			}

			select {
			case <-p.done:
				return
			case finish <- &TaskFinished{
				*task, p.storer.StorerType(), p.storer.StorerKey(), washedMIMEType,
			}:
			}
		}
	}()

	return
}