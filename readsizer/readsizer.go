package readsizer
import (
	"io"
	"errors"
	"bytes"
)

type ByteSize int64

const (
	_           = iota // ignore first value by assigning to blank identifier
	KB ByteSize = 1 << (10 * iota)
	MB
	GB
)

type sizeLimitedReader struct {
	r io.Reader
	limit int64
	cnt int64
}

func newSizeLimitedReader(r io.Reader, limit ByteSize) *sizeLimitedReader {
	return &sizeLimitedReader{r, (int64)(limit), 0}
}

func (p *sizeLimitedReader) Read(b []byte) (n int, err error) {
	n, err = p.r.Read(b)
	if err != nil {
		return
	}
	p.cnt += (int64)(n)
	if p.cnt > p.limit {
		err = errors.New("size exceed")
	}
	return
}

type ReadSizer interface {
	io.Reader
	Size() int64
}

func ReadCloserToReadSizer(r io.ReadCloser, MaxMeasureSize ByteSize) (ReadSizer, error) {
	defer r.Close()
	buf := bytes.NewBuffer(make([]byte, 0, 512))

	n, err := buf.ReadFrom(newSizeLimitedReader(r, MaxMeasureSize))
	if err != nil {
		return nil, err
	}

	return newRSFromR(buf, n), nil
}

func newRSFromR(r io.Reader, size int64) (ReadSizer) {
	return &_RSFromR { r, size }
}

type _RSFromR struct {
	io.Reader
	size int64
}

func (p *_RSFromR) Size() int64 {
	return p.size
}