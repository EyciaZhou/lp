package pic
import (
	"github.com/EyciaZhou/picRouter/readsizer"
	"github.com/EyciaZhou/picRouter/qiniu"
)

type Storer interface {
	StorerType() string
	StorerKey() string

	Store(r readsizer.ReadSizer, key string) error
	StoreFile(fn string, key string) error
}

type QiniuStorer qiniu.QiniuUploader

func (p *QiniuStorer) StorerType() string {
	return "QINIU"
}

func (p *QiniuStorer) StorerKey() string {
	return p.Bucket
}

func (p *QiniuStorer) Store(r readsizer.ReadSizer, key string) error {
	return (* qiniu.QiniuUploader)(p).Upload(r, r.Size(), key)
}

func (p *QiniuStorer) StoreFile(fn string, key string) error {
	return (* qiniu.QiniuUploader)(p).UploadFile(fn, key)
}

type QiniuStorerConf struct {
	AccessKey string
	SecretKey string
	Bucket string
}

func NewQiniuStorer(conf *QiniuStorerConf) (*QiniuStorer) {
	return (* QiniuStorer)(qiniu.NewQiniuUploader(
		conf.AccessKey, conf.SecretKey, conf.Bucket,
	))
}