package file

import (
	"os"
	"path/filepath"

	"github.com/dgrr/rfs"
)

func makeFs(root string, config rfs.Config) (rfs.Fs, error) {
	return &Fs{
		Root: root,
	}, nil
}

// Fs ...
type Fs struct {
	Root string
}

// Name ...
func (fs *Fs) Name() string {
	return fs.Root
}

func (fs *Fs) joinRoot(path string) string {
	return filepath.Join(fs.Root, path)
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
		filepath.Join(fs.Root, path),
	)
	if err == nil {
		return rfs.Stat{
			StatName: st.Name(),
			StatSize: st.Size(),
		}, nil
	}
	return nil, err
}
