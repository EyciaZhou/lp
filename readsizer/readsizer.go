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

type SizeLimitReader struct {
	r io.Reader
	limit int64
	cnt int64
}

func NewSizeLimitReader(r io.Reader, limit int64) *SizeLimitReader {
	return &SizeLimitReader{r, limit, 0}
}

type ReadSizer interface {
	io.Reader
	Size() int64
}

func (p *SizeLimitReader) Read(b []byte) (n int, err error) {
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

type MemoryReadSizer struct {
	r bytes.Reader
	size int64
}

func NewSizeReader(r io.Reader) (*ReadSizer, error) {
	if rs, ok := r.(ReadSizer); ok {
		return rs, nil
	}

	buf := bytes.NewBuffer(make([]byte, 0, 512))

	n, err := buf.ReadFrom(r)
	if err != nil {
		return nil, err
	}

	return &MemoryReadSizer { buf, n }, nil
}

func NewSizeReaderWithSize(r io.Reader, size int64) (*ReadSizer, error) {
	return &MemoryReadSizer{ r, size }, nil
}

func (p *MemoryReadSizer) Size() (int64) {
	return p.size
}
