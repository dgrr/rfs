package s3

import "github.com/digilant/rfs"

const (
	Kind = "s3"
	ETag = "etag"
)

const (
	KeyID        = "access_key"
	SecretID     = "secret_key"
	Region       = "region"
	Profile      = "profile"
	SessionToken = "session_token"
)

func init() {
	rfs.Register(Kind, makeFs)
}
