package rfs

import (
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
)

// File represents a file abstraction.
type File interface {
	// io.Closer implements the Close() function.
	io.Closer

	// io.Writer implements the Write() function.
	io.Writer

	// io.Reader implements the Read() function.
	io.Reader

	// URL returns the file url.
	URL() *url.URL
}

// Stat TODO
type Stat map[string]interface{}

// Fs represents the filesystem abstraction.
type Fs interface {
	// Name returns the filesytem name.
	Name() string

	// Open returns a read-only file.
	Open(path string) (File, error)

	// Create a write-only file.
	//
	// If the path where the file is located doesn't exits
	// it will be created automatically.
	Create(path string) (File, error)

	// Remove removes the specified file.
	Remove(path string) error

	// RemoveAll removes all the files and directories recursively.
	RemoveAll(path string) error

	// TODO: Stat(path string) (Stat, error)
}

// Config ...
type Config map[string]string

var (
	fsMap = make(map[string]MakeFunc)
)

// Dial configures the filesystem based on the kind, root and config parameters.
//
// kind is based on *.Kind where `*` is the name of the subpackage like s3 or file.
// root is the root object (in S3 is the bucket). In case of `file` subpackages you can left it blank.
// config is a map dependent on the subpackage constants.
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

// DialURL does the same as Dial but using a URL.
//
// Is basically an alias of Dial(uri.Scheme, uri.Host, config)
func DialURL(uri *url.URL, config Config) (Fs, error) {
	if len(uri.Host) == 0 && filepath.IsAbs(uri.Path) {
		uri.Host = getRoot(uri.Path)
	}
	return Dial(uri.Scheme, uri.Host, config)
}

// Open returns a File avoiding Fs handling.
//
// The fileURI parameter is a string with the file location, for example:
// s3://my-bucket/my/file/path or
// file:///tmp/my/file/path
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

// Create returns a File avoiding Fs handling.
//
// The fileURI parameter is a string with the file location, for example:
// s3://my-bucket/my/file/path or
// file:///tmp/my/file/path
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

// Register registers a new kind on the rfs package.
func Register(kind string, mfn MakeFunc) {
	fsMap[kind] = mfn
}
