package sweeper

import (
	"errors"
	"io"
	"strings"
)

const (
	defaultBufSize           = 256
	minReadBufferSize        = 1
	maxConsecutiveEmptyReads = 100
)

var (
	ErrInvalidUnreadByte = errors.New("sweeper: invalid use of UnreadByte")
	ErrInvalidUnreadRune = errors.New("sweeper: invalid use of UnreadRune")
	ErrBufferFull        = errors.New("sweeper: buffer full")
	ErrNegativeCount     = errors.New("sweeper: negative count")

	errNegativeRead = errors.New("sweeper: reader returned negative count from Read")
)

// Reader implements buffering for an io.Reader object.
type Sweeper struct {
	buf          []byte
	rd           io.Reader // reader provided by the client
	r, w         int       // buf read and write positions
	err          error
	lastByte     int // last byte read for UnreadByte; -1 means invalid
	lastRuneSize int // size of last rune read for UnreadRune; -1 means invalid
}

// NewSweeperSize returns a new Sweeper whose buffer has at least the specified
// size. If the argument io.Reader is already a Reader with large enough
// size, it returns the underlying Sweeper.
func NewSweeperSize(rd io.Reader, size int) *Sweeper {
	// Is it already a Reader?
	b, ok := rd.(*Sweeper)
	if ok && len(b.buf) >= size {
		return b
	}
	if size < minReadBufferSize {
		size = minReadBufferSize
	}
	r := new(Sweeper)
	r.reset(make([]byte, size), rd)
	return r
}

// NewSweeper returns a new Sweeper whose buffer has the default size.
func NewSweeper(rd io.Reader) *Sweeper {
	return NewSweeperSize(rd, defaultBufSize)
}

// Size returns the size of the underlying buffer in bytes.
func (b *Sweeper) Size() int { return len(b.buf) }

// Buffered returns the number of bytes that can be read from the current buffer.
func (b *Sweeper) Buffered() int { return b.w - b.r }

// Reset discards any buffered data, resets all state, and switches
// the buffered reader to read from r.
func (b *Sweeper) Reset(r io.Reader) {
	b.reset(b.buf, r)
}

func (b *Sweeper) reset(buf []byte, r io.Reader) {
	*b = Sweeper{
		buf:          buf,
		rd:           r,
		lastByte:     -1,
		lastRuneSize: -1,
	}
}

func (b *Sweeper) readErr() error {
	err := b.err
	b.err = nil
	return err
}

func (b *Sweeper) fillSingleByte() {
	temp := b.buf
	b.buf = make([]byte, len(temp)+1)

	copy(b.buf, temp)

	// if the read position is greater than zero then the delimiter was found.
	if b.r > 0 {
		// Since the delimiter was found we may reset the buffer back to its
		// original size to clean up.
		temp := b.buf[b.r:]
		b.buf = make([]byte, defaultBufSize)
		copy(b.buf, temp)

		// Just set the read and write positions to 0 so then it can scan
		// from the beginning of the slice when it begins again.
		// b.w = 0
		b.w = len(temp) - 1
		b.r = 0
	}

	if b.w >= len(b.buf) {
		panic("bufio: tried to fill full buffer")
	}

	// Read new data: try a limited number of times.
	for i := maxConsecutiveEmptyReads; i > 0; i-- {
		// Reads the length of the data that's not part of the already
		// existing data that I appended earlier. This means that it will
		// search a total of one byte in this function call.
		n, err := b.rd.Read(b.buf[b.w:])
		if n < 0 {
			panic(errNegativeRead)
		}

		// println(b.buf[b.w:])

		b.w += n
		if err != nil {
			b.err = err
			return
		}
		if n > 0 {
			return
		}
	}
	b.err = io.ErrNoProgress
}

// ReadSliceWithString reads until the first occurrence of delim in the input,
// returning a slice pointing at the bytes in the buffer.
// The bytes stop being valid at the next read.
// If ReadSlice encounters an error before finding a delimiter,
// it returns all the data in the buffer and the error itself (often io.EOF).
// ReadSlice fails with error ErrBufferFull if the buffer fills without a delim.
// Because the data returned from ReadSlice will be overwritten
// by the next I/O operation, most clients should use
// ReadBytes or ReadString instead.
// ReadSlice returns err != nil if and only if line does not end in delim.
func (b *Sweeper) ReadSliceWithString(delim string) (line []byte, err error) {
	var s = -1
	for {
		// Search buffer.
		// Change made to ReadSlice to accept string ----------------------- Damien S.
		if s >= 0 {
			if i := strings.Index(string(b.buf[b.r+s:]), delim); i >= 0 {
				line = b.buf[:i+len(delim)]
				// b.r = b.w
				b.r = i + len(delim)

				break
			}
		}

		// Pending error?
		if b.err != nil && b.err != io.EOF {
			line = b.buf[b.r:b.w]
			b.r = b.w
			err = b.readErr()
			break
		}

		// Note: This function does not check the buffered size in comparison
		// to the length of the buffer, because b.r is always zero and never
		// incremented since we are rescanning all of the buffer all of the time.

		s++

		if b.err != io.EOF {
			b.fillSingleByte()
		} else {
			temp := b.buf[b.r:]
			var checkNil []bool
			for _, tempByte := range temp {
				if tempByte == 0 {
					checkNil = append(checkNil, true)
				}
			}
			if len(checkNil) == len(temp) {
				line = b.buf
				b.r = b.w
				err = b.err
				break
			}
			b.buf = make([]byte, len(temp))

			copy(b.buf, temp)

			b.r = 0
			s = 0
		}
	}

	// Handle last byte, if any.
	if i := len(line) - 1; i >= 0 {
		b.lastByte = int(line[i])
		b.lastRuneSize = -1
	}

	return
}

// Read reads data into p.
// It returns the number of bytes read into p.
// The bytes are taken from at most one Read on the underlying Reader,
// hence n may be less than len(p).
// To read exactly len(p) bytes, use io.ReadFull(b, p).
// At EOF, the count will be zero and err will be io.EOF.
func (b *Sweeper) Read(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		return 0, b.readErr()
	}
	if b.r == b.w {
		if b.err != nil {
			return 0, b.readErr()
		}
		if len(p) >= len(b.buf) {
			// Large read, empty buffer.
			// Read directly into p to avoid copy.
			n, b.err = b.rd.Read(p)
			if n < 0 {
				panic(errNegativeRead)
			}
			if n > 0 {
				b.lastByte = int(p[n-1])
				b.lastRuneSize = -1
			}
			return n, b.readErr()
		}
		// One read.
		// Do not use b.fill, which will loop.
		b.r = 0
		b.w = 0
		n, b.err = b.rd.Read(b.buf)
		if n < 0 {
			panic(errNegativeRead)
		}
		if n == 0 {
			return 0, b.readErr()
		}
		b.w += n
	}

	// copy as much as we can
	n = copy(p, b.buf[b.r:b.w])
	b.r += n
	b.lastByte = int(b.buf[b.r-1])
	b.lastRuneSize = -1
	return n, nil
}