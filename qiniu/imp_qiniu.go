package qiniu

import (
	"qiniupkg.com/api.v7/kodo"
	"qiniupkg.com/api.v7/kodocli"
	"io"
)

type QiniuUploader struct {
	bucket string `default:"msghub-picture"`
	c *kodo.Client
	policy *kodo.PutPolicy
}

func NewQiniuUploader(AccessKey string, SecretKey string, Bucket string) (*QiniuUploader) {
	return &QiniuUploader{
		Bucket,
		kodo.New(0, &kodo.Config{
			AccessKey:AccessKey,
			SecretKey:SecretKey,
		}),

		&kodo.PutPolicy{
			Scope: Bucket,
			Expires: 3600,
		},
	}
}

func (q *QiniuUploader) UploadFile(fn string, key string) error {
	token := q.c.MakeUptoken(q.policy)
	uploader := kodocli.NewUploader(0, nil)
	return uploader.PutFile(nil, nil, token, key, fn, nil)
}

func (q *QiniuUploader) Upload(r io.Reader, size int64, key string) error {
	token := q.c.MakeUptoken(q.policy)
	uploader := kodocli.NewUploader(0, nil)
	return uploader.Put(nil, nil, token, key, r, size, nil)
}