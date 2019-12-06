package file

import (
	"fmt"
	"net/url"
	"os"
)

// File ...
type File struct {
	path string
	*os.File
}

// URL ...
func (f *File) URL() *url.URL {
	uri, err := url.Parse(
		fmt.Sprintf("%s://%s", Kind, f.path),
	)
	if err != nil {
		return nil
	}
	return uri
}
