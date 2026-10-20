package proxy

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

var errBodyLimit = errors.New("body exceeds configured limit")

// A bounded channel retains at most one fixed-size buffer per admitted request.
// Unlike sync.Pool, the retention bound is explicit and independent of GC timing.
type buffers struct {
	free chan []byte
	size int
}

func newBuffers(count, size int) *buffers { return &buffers{make(chan []byte, count), size + 1} }
func (b *buffers) get() []byte {
	select {
	case v := <-b.free:
		return v
	default:
		return make([]byte, b.size)
	}
}
func (b *buffers) put(v []byte) {
	select {
	case b.free <- v:
	default:
	}
}

func readBounded(r io.Reader, buf []byte) (int, error) {
	n, empty := 0, 0
	for n < len(buf) {
		k, e := r.Read(buf[n:])
		n += k
		if n == len(buf) {
			return n, errBodyLimit
		}
		if e == io.EOF {
			return n, nil
		}
		if e != nil {
			return n, e
		}
		if k == 0 {
			empty++
			if empty >= 100 {
				return n, io.ErrNoProgress
			}
		} else {
			empty = 0
		}
	}
	return n, errBodyLimit
}

type bufferedBody struct {
	*bytes.Reader
	pool *buffers
	data []byte
	once sync.Once
}

func (b *bufferedBody) Close() error { b.once.Do(func() { b.pool.put(b.data) }); return nil }
