package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3aws "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	fiveMB = 5 * 1024 * 1024
)

// File represents an abstract file.
type File struct {
	bucket   string
	path     string
	meta     *FileInfo
	cursor   int64
	uploadID string
	c        *s3aws.Client
	b        []byte // buffered content
}

// like virtual func
func (f *File) Read(_ []byte) (int, error) {
	return -1, errors.New("file not open for reading")
}

// like virtual func
func (f *File) ReadAt(_ []byte, _ int64) (int, error) {
	return -1, errors.New("file not open for reading")
}

// like virtual func
func (f *File) Write(_ []byte) (int, error) {
	return -1, errors.New("file not open for writing")
}

// FileReader ...
type FileReader struct {
	File
}

// FileWriter ...
type FileWriter struct {
	File
	partNum int32
	size    int64
	cmpl    types.CompletedMultipartUpload
}

// NewReader ...
func NewReader(c *s3aws.Client) *FileReader {
	f := &FileReader{}
	f.meta = new(FileInfo)
	f.c = c
	return f
}

// NewWriter ...
func NewWriter(c *s3aws.Client) *FileWriter {
	f := &FileWriter{
		partNum: 1,
		size:    fiveMB,
	}
	f.b = make([]byte, fiveMB)
	f.meta = new(FileInfo)
	f.c = c

	return f
}

// URL ...
func (f *File) URL() *url.URL {
	uri, err := url.Parse(
		fmt.Sprintf("%s://%s/%s", Kind, f.bucket, f.path),
	)
	if err == nil {
		return uri
	}
	return nil
}

// Read ...
func (f *FileReader) Read(b []byte) (int, error) {
	if f.cursor == f.meta.size {
		return 0, io.EOF
	}

	bLen := len(f.b)
	cped := copy(b, f.b)
	f.b = append(f.b[:0], f.b[cped:]...)
	f.cursor += int64(cped)

	if bLen >= len(b) {
		return cped, nil
	}

	nextMB := f.cursor + fiveMB
	if nextMB > f.meta.size {
		nextMB = f.meta.size
	}

	f.b = resize(f.b, 1+nextMB-f.cursor)

	n, err := f.readRange(f.b, f.cursor, nextMB)
	if err == nil {
		n = copy(b[cped:], f.b)
		f.b = append(f.b[:0], f.b[n:]...)
		f.cursor += int64(n)
	}

	return cped + n, err
}

// ReadAt ...
func (f *FileReader) ReadAt(p []byte, off int64) (int, error) {
	return f.readAt(p, off)
}

func (f *FileReader) readAt(b []byte, offset int64) (int, error) {
	if f.c == nil {
		return 0, io.ErrClosedPipe
	}

	max := offset + int64(len(b)-1)
	if max > f.meta.size {
		max = f.meta.size
	}

	return f.readRange(b, offset, max)
}

func (f *FileReader) readRange(b []byte, offset, max int64) (int, error) {
	resp, err := f.c.GetObject(context.Background(),
		&s3aws.GetObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.path),
			Range: aws.String(
				fmt.Sprintf("bytes=%d-%d", offset, max),
			),
		})
	if err != nil {
		return 0, err
	}

	n, err := io.Copy(&byteWriter{b, 0}, resp.Body)
	resp.Body.Close()

	return int(n), err
}

// Seek ...
func (f *FileReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.cursor = offset
	case io.SeekCurrent:
		f.cursor += offset
		offset = f.cursor
	case io.SeekEnd:
		return 0, errors.New("cannot seek at the end of the file")
	}

	return offset, nil
}

// Write ...
func (f *FileWriter) Write(b []byte) (n int, err error) {
	if f.c == nil {
		return -1, io.ErrClosedPipe
	}
	if f.cursor == f.size {
		err = f.Flush()
		if err == nil {
			f.cursor = 0
		}
	}
	if err != nil {
		return -1, err
	}

	n = copy(f.b[f.cursor:], b)
	f.cursor += int64(n)

	return n, err
}

// Seek ...
func (f *FileWriter) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.cursor = offset
	case io.SeekCurrent:
		f.cursor += offset
		offset = f.cursor
	}

	return offset, nil
}

// Flush ...
func (f *FileWriter) Flush() error {
	size := f.cursor
	partNum := f.partNum

	resp, err := f.c.UploadPart(
		context.Background(),
		&s3aws.UploadPartInput{
			Bucket:        aws.String(f.bucket),
			Key:           aws.String(f.path),
			Body:          bytes.NewReader(f.b[:size]),
			ContentLength: aws.Int64(int64(size)),
			UploadId:      aws.String(f.uploadID),
			PartNumber:    aws.Int32(partNum),
		},
	)
	if err != nil {
		return fmt.Errorf("Flush(): %s", err)
	}
	f.cmpl.Parts = append(f.cmpl.Parts, &types.CompletedPart{
		ETag:       resp.ETag,
		PartNumber: &partNum,
	})

	f.partNum++

	return nil
}

// Close ...
func (f *FileReader) Close() error {
	if f.c == nil {
		return io.ErrClosedPipe
	}
	f.c = nil

	return nil
}

// Close ...
func (f *FileWriter) Close() error {
	if f.c == nil {
		return io.ErrClosedPipe
	}

	err := f.Flush()
	if err != nil {
		return err
	}
	resp, err := f.c.CompleteMultipartUpload(
		context.Background(),
		&s3aws.CompleteMultipartUploadInput{
			Bucket:          aws.String(f.bucket),
			Key:             aws.String(f.path),
			UploadId:        aws.String(f.uploadID),
			MultipartUpload: &f.cmpl,
		},
	)
	if err != nil {
		return fmt.Errorf("Close(): %s", err)
	}
	f.c = nil
	f.bucket = aws.ToString(resp.Bucket)
	f.path = aws.ToString(resp.Key)
	f.meta.hash = aws.ToString(resp.ETag)

	return nil
}

// Stat ...
func (f *File) Stat() (os.FileInfo, error) {
	var err error
	if f.meta.isEmpty() {
		err = f.stat()
	}
	return f.meta, err
}

func (f *File) stat() error {
	st, err := stat(f.c, f.bucket, f.path)
	if err == nil {
		f.meta = st
	}

	return err
}
