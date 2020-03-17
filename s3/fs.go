package s3

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/digilant/rfs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3aws "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
)

func getAWSConfig(region, profile string) (aws.Config, error) {
	var options []external.Config
	if len(region) > 0 {
		options = append(options, external.WithRegion(region))
	}
	if len(profile) > 0 {
		options = append(
			options, external.WithSharedConfigProfile(profile),
		)
	}

	return external.LoadDefaultAWSConfig(options...)
}

func makeFs(bucket string, config rfs.Config) (rfs.Fs, error) {
	var (
		awsConfig aws.Config
		err       error
		region    = config[Region]
	)
	if len(region) == 0 {
		region = "us-east-1"
	}
	awsConfig.Region = region

	_, dontUseFile := config[KeyID]
	if !dontUseFile { // so use file
		var (
			profile string
			ok      bool
		)
		if profile, ok = config[Profile]; !ok {
			profile = "default"
		}
		awsConfig, err = getAWSConfig(region, profile)
	} else {
		awsConfig.Credentials = aws.NewStaticCredentialsProvider(
			config[KeyID], config[SecretID], config[SessionToken],
		)
	}
	if err != nil {
		return nil, err
	}

	region, err = s3manager.GetBucketRegion(context.Background(), awsConfig, bucket, region)
	if err == nil && len(region) > 0 {
		awsConfig.Region = region
	}

	c := s3aws.New(awsConfig)

	return &Fs{
		bucket: bucket,
		c:      c,
	}, nil
}

// Fs implements the interface rfs.Fs
type Fs struct {
	bucket string
	c      *s3aws.Client
}

// Name returns the bucket name.
func (fs *Fs) Name() string {
	return "s3"
}

// Root ...
func (fs *Fs) Root() string {
	return fs.bucket
}

// Stat ...
func (fs *Fs) Stat(path string) (os.FileInfo, error) {
	return stat(fs.c, fs.bucket, path)
}

func stat(c *s3aws.Client, bucket, path string) (*FileInfo, error) {
	path = cleanPath(path)

	req := c.HeadObjectRequest(
		&s3aws.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path),
		},
	)

	res, err := req.Send(context.Background())
	if err != nil {
		return nil, err
	}

	return &FileInfo{
		name:    path,
		size:    aws.Int64Value(res.ContentLength),
		modtime: aws.TimeValue(res.LastModified),
	}, nil
}

func cleanPath(path string) string {
	if filepath.IsAbs(path) {
		path = path[1:]
	}
	return path
}

// Open ...
func (fs *Fs) Open(path string) (rfs.File, error) {
	path = cleanPath(path)

	file := NewReader(fs.c)
	{
		file.bucket = fs.bucket
		file.path = path
	}

	return file, file.stat()
}

// Create ...
func (fs *Fs) Create(path string) (rfs.File, error) {
	path = cleanPath(path)

	file := NewWriter(fs.c)
	{
		file.bucket = fs.bucket
		file.path = path
	}

	resp, err := fs.c.CreateMultipartUploadRequest(
		&s3aws.CreateMultipartUploadInput{
			Bucket: aws.String(fs.bucket),
			Key:    aws.String(path),
		},
	).Send(context.Background())
	if err != nil {
		return nil, err
	}
	file.uploadID = aws.StringValue(resp.UploadId)

	return file, nil
}

// Remove ...
func (fs *Fs) Remove(path string) error {
	path = cleanPath(path)

	_, err := fs.c.DeleteObjectRequest(&s3aws.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(path),
	}).Send(context.Background())
	// TODO: check response.DeleteMarker ?
	return err
}

// RemoveAll TODO
func (fs *Fs) RemoveAll(path string) error {
	return nil
}

func (fs *Fs) ListDir(path string) ([]string, error) {
	path = cleanPath(path)

	files := make([]string, 0)
	for {
		req := fs.c.ListObjectsV2Request(
			&s3.ListObjectsV2Input{
				Bucket:    aws.String(fs.bucket),
				Prefix:    aws.String(path),
				Delimiter: aws.String("/"),
			},
		)

		res, err := req.Send(context.Background())
		if err != nil {
			return nil, err
		}
		if len(res.Contents) == 0 {
			break
		}

		for _, object := range res.Contents {
			files = append(files, aws.StringValue(object.Key))
		}
	}

	return files, nil
}

func (fs *Fs) WalkDepth(root string, depth int, walkFn rfs.WalkFunc) error {
	return fs.walk(root, depth, walkFn)
}

// Walk walks the file tree rooted at root, calling walkFn for each file or directory
// in the tree, including root. All errors that arise visiting files and directories are
// filtered by walkFn.
func (fs *Fs) Walk(root string, walkFn rfs.WalkFunc) error {
	return fs.walk(root, -1, walkFn)
}

func (fs *Fs) walk(root string, depth int, walkFn rfs.WalkFunc) (err error) {
	root = cleanPath(root)

	mustBreak := false
	last := ""
	for err == nil && !mustBreak {

		req := fs.c.ListObjectsV2Request(
			&s3.ListObjectsV2Input{
				Bucket:     aws.String(fs.bucket),
				Prefix:     aws.String(root),
				StartAfter: aws.String(last),
			},
		)

		res, er := req.Send(context.Background())
		if er != nil {
			err = er
		}
		if len(res.Contents) == 0 {
			break
		}

		if err == nil {
			for _, object := range res.Contents {
				path := aws.StringValue(object.Key)

				if depth >= 0 {
					look, er := filepath.Rel(root, path)
					if er != nil {
						look = path
					}
					// TODO: Doesn't work so well... println(look, depth, strings.Count(look, "/"))

					mustBreak = strings.Count(look, "/") > depth
					if mustBreak {
						break
					}
				}

				err = walkFn(path, false)
				if err != nil {
					break
				}
				last = path
			}
		}
	}

	return
}
