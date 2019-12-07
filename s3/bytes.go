package s3

import "io"

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
