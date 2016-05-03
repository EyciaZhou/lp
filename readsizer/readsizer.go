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

type ReadCloseSizer interface {
	io.ReadCloser
	Size() int64
}

func NewReadCloseSizerByMeasureSize(r io.Reader, MaxMeansureSize ByteSize) (ReadCloseSizer, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 512))

	n, err := buf.ReadFrom(newSizeLimitedReader(r, MaxMeansureSize))
	if err != nil {
		return nil, err
	}

	return NewRCSFromR(r, n, func()error { return nil }), nil
}

func NewRCSFromRC(r io.ReadCloser, size int64) ReadCloseSizer {
	return &_RCSFromRC{ r, size }
}

type _RCSFromRC struct {
	io.ReadCloser
	size int64
}

func (p *_RCSFromRC) Size() int64 {
	return p.size
}

func NewRCSFromR(r io.Reader, size int64, closeFunc func()error) (ReadCloseSizer) {
	return &_RCSFromR { r, size, closeFunc }
}

type _RCSFromR struct {
	io.Reader
	size int64
	closeFunc func()error
}

func (p *_RCSFromR) Close() error {
	return p.closeFunc()
}

func (p *_RCSFromR) Size() int64 {
	return p.size
}