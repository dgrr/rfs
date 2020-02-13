package file

import "github.com/digilant/rfs"

const (
	// Kind ...
	Kind = "file"
)

func init() {
	rfs.Register(Kind, makeFs)
}
