package s3

import (
	"errors"
	"fmt"
	"io"
	"net/url"
)

// File ...
type File struct {
	path string
	meta map[string]interface{}
	size int64
	r    io.ReadCloser
	w    io.WriteCloser
}

// URL ...
func (f *File) URL() *url.URL {
	uri, err := url.Parse(
		fmt.Sprintf("%s://%s", Kind, f.path),
	)
	if err == nil {
		return uri
	}
	return nil
}

// Read ...
func (f *File) Read(b []byte) (int, error) {
	if f.r == nil {
		return -1, errors.New("file not opened for reading")
	}
	return f.r.Read(b)
}

// Write ...
func (f *File) Write(b []byte) (int, error) {
	if f.w == nil {
		return -1, errors.New("file not opened for writing")
	}
	return f.w.Write(b)
}

// Close ...
func (f *File) Close() error {
	if f.r != nil {
		return f.r.Close()
	}
	if f.w == nil {
		return errors.New("file not opened")
	}
	return f.w.Close()
}
