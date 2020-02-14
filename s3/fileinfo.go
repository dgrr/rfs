package s3

import (
	"os"
	"time"
)

type FileInfo struct {
	name    string
	size    int64
	hash    string
	modtime time.Time
}

// Name returns the name of the file
func (fi *FileInfo) Name() string {
	return fi.name
}

// Size returns the size of the file
func (fi *FileInfo) Size() int64 {
	return fi.size
}

func (fi *FileInfo) Mode() os.FileMode {
	return 0
}

func (fi *FileInfo) ModTime() time.Time {
	return fi.modtime
}

func (fi *FileInfo) IsDir() bool {
	return false
}

func (fi *FileInfo) ETag() string {
	return fi.hash
}

func (fi *FileInfo) Sys() interface{} {
	return nil
}
