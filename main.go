package rfs

import (
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
)

// File ...
type File interface {
	io.Closer
	io.Writer
	io.Reader

	URL() *url.URL
}

// Stat ...
type Stat map[string]interface{}

// Fs ...
type Fs interface {
	Name() string

	Open(path string) (File, error)

	Create(path string) (File, error)

	Remove(path string) error

	RemoveAll(path string) error

	// TODO: Stat(path string) (Stat, error)
}

// Config ...
type Config map[string]string

var (
	fsMap = make(map[string]MakeFunc)
)

// Dial ...
func Dial(kind, root string, config Config) (Fs, error) {
	mfn, ok := fsMap[kind]
	if !ok {
		return nil, fmt.Errorf("`%s` filesystem not found", kind)
	}
	return mfn(root, config)
}

func getRoot(path string) string {
	path = filepath.Clean(path)
	i := strings.IndexByte(path, filepath.Separator)
	if i == -1 {
		return path
	}
	if i == 0 {
		i = strings.IndexByte(path[1:], filepath.Separator)
		if i == -1 {
			return path
		}
		i++
	}
	return path[:i]
}

// DialURL ...
func DialURL(uri *url.URL, config Config) (Fs, error) {
	if len(uri.Host) == 0 && filepath.IsAbs(uri.Path) {
		uri.Host = getRoot(uri.Path)
	}
	return Dial(uri.Scheme, uri.Host, config)
}

// Open ...
func Open(fileURI string, config Config) (File, error) {
	uri, err := url.Parse(fileURI)
	if err != nil {
		return nil, err
	}

	fs, err := Dial(uri.Scheme, uri.Host, config)
	if err == nil {
		return fs.Open(uri.Path)
	}

	return nil, err
}

// Create ...
func Create(fileURI string, config Config) (File, error) {
	uri, err := url.Parse(fileURI)
	if err != nil {
		return nil, err
	}

	fs, err := Dial(uri.Scheme, uri.Host, config)
	if err == nil {
		return fs.Create(uri.Path)
	}

	return nil, err
}

type (
	// MakeFunc ...
	MakeFunc func(root string, config Config) (Fs, error)
)

// Register ...
func Register(kind string, mfn MakeFunc) {
	fsMap[kind] = mfn
}
