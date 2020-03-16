package s3

import (
	"io"
)

func resize(b []byte, needed int64) []byte {
	b = b[:cap(b)]
	if n := needed - int64(cap(b)); n > 0 {
		b = append(b, make([]byte, n)...)
	}
	return b[:needed]
}

type byteWriter struct {
	b []byte
	n int
}

func (bw *byteWriter) Write(b []byte) (int, error) {
	if bw.n == len(bw.b) {
		return 0, io.EOF
	}
	n := copy(bw.b[bw.n:], b)
	bw.n += n
	return n, nil
}
