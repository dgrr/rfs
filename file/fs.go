package file

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/digilant/rfs"
)

func makeFs(root string, config rfs.Config) (rfs.Fs, error) {
	return &Fs{
		root: root,
	}, nil
}

// Fs ...
type Fs struct {
	root string
}

// Name ...
func (fs *Fs) Name() string {
	return "file"
}

func (fs *Fs) Root() string {
	return fs.root
}

func (fs *Fs) joinRoot(path string) string {
	return filepath.Join(fs.root, path)
}

// Open ...
func (fs *Fs) Open(path string) (rfs.File, error) {
	path = fs.joinRoot(path)
	file, err := os.Open(path)
	if err == nil {
		return &File{
			path: path,
			File: file,
		}, nil
	}
	return nil, err
}

// Create ...
func (fs *Fs) Create(path string) (rfs.File, error) {
	path = fs.joinRoot(path)

	_, err := os.Stat(filepath.Dir(path))
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(
			filepath.Dir(path), 0600,
		)
	}

	if err == nil {
		file, err := os.Create(path)
		if err == nil {
			return &File{
				path: path,
				File: file,
			}, nil
		}
	}
	return nil, err
}

// Remove ...
func (fs *Fs) Remove(path string) error {
	return os.Remove(
		fs.joinRoot(path),
	)
}

// RemoveAll ...
func (fs *Fs) RemoveAll(path string) error {
	return os.RemoveAll(
		fs.joinRoot(path),
	)
}

const (
	StatName = "Name"
	StatSize = "Size"
)

// Stat ...
func (fs *Fs) Stat(path string) (rfs.Stat, error) {
	st, err := os.Stat(
		filepath.Join(fs.root, path),
	)
	if err == nil {
		return rfs.Stat{
			StatName: st.Name(),
			StatSize: st.Size(),
		}, nil
	}
	return nil, err
}

func (fs *Fs) ListDir(path string) ([]string, error) {
	path = filepath.Join(fs.root, path)
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	files, err := file.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	for i := range files {
		files[i] = filepath.Join(fs.root, path, files[i])
	}

	return files, nil
}

func (fs *Fs) WalkDepth(path string, depth int, walkFn rfs.WalkFunc) error {
	return fs.walk(path, depth, walkFn)
}

func (fs *Fs) Walk(path string, walkFn rfs.WalkFunc) error {
	return fs.walk(path, -1, walkFn)
}

func (fs *Fs) walk(path string, depth int, walkFn rfs.WalkFunc) error {
	err := filepath.Walk(filepath.Join(fs.root, path), func(path string, info os.FileInfo, _ error) error {
		if depth >= 0 {
			look, err := filepath.Rel(fs.root, path)
			if err != nil {
				look = path
			}

			if strings.Count(look, "/") <= depth {
				return walkFn(path, info.IsDir())
			}

			return nil
		}

		return walkFn(path, info.IsDir())
	})
	if err == io.EOF {
		err = nil
	}

	return err
}
