package s3

import (
	"context"
	"path/filepath"

	"github.com/dgrr/rfs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
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
	awsConfig.Region = region

	if profile, ok := config[Profile]; ok {
		awsConfig, err = getAWSConfig(region, profile)
	} else {
		awsConfig.Credentials = aws.NewStaticCredentialsProvider(
			config[KeyID], config[SecretID], config[SessionToken],
		)
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

// Fs ...
type Fs struct {
	bucket string
	c      *s3aws.Client
}

// Name returns the bucket name.
func (fs *Fs) Name() string {
	return fs.bucket
}

// Open ...
func (fs *Fs) Open(path string) (rfs.File, error) {
	if filepath.IsAbs(path) {
		path = path[1:]
	}
	input := s3aws.GetObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(path),
	}
	resp, err := fs.c.GetObjectRequest(&input).Send(context.Background())
	if err != nil {
		return nil, err
	}

	file := &FileReader{}
	{
		file.c = fs.c
		file.path = filepath.Join(fs.bucket, path)
		file.meta = make(map[string]interface{})
		file.size = aws.Int64Value(resp.ContentLength)

		for k, v := range resp.Metadata {
			file.meta[k] = v
		}
		file.meta[ETag] = aws.StringValue(resp.ETag)
	}

	return file, nil
}

// Create ...
func (fs *Fs) Create(path string) (rfs.File, error) {
	if filepath.IsAbs(path) {
		path = path[1:]
	}

	file := NewWriter(fs.c)
	{
		file.bucket = fs.bucket
		file.path = path
	}

	resp, err := fs.c.CreateMultipartUploadRequest(&s3aws.CreateMultipartUploadInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(path),
	}).Send(context.Background())
	if err != nil {
		return nil, err
	}
	file.uploadID = aws.StringValue(resp.UploadId)

	return file, nil
}

// Remove ...
func (fs *Fs) Remove(path string) error {
	return nil
}

// RemoveAll ...
func (fs *Fs) RemoveAll(path string) error {
	return nil
}
