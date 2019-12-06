package file

import "github.com/dgrr/rfs"

const (
	// Kind ...
	Kind = "file"
)

func init() {
	rfs.Register(Kind, makeFs)
}
