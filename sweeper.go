package sweeper

import (
	"bytes"
	"errors"
	"io"
)

const (
	defaultBufSize           = 256
	minReadBufferSize        = 1
	maxConsecutiveEmptyReads = 100
)

var (
	errNegativeRead = errors.New("sweeper: reader returned negative count from Read")
)

// Sweeper implements buffering for an io.Reader object.
type Sweeper struct {
	buf  []byte
	rd   io.Reader // reader provided by the client
	r, w int       // buf read and write positions
	err  error
}

// NewSweeperSize returns a new Sweeper whose buffer has at least the specified
// size. If the argument io.Reader is already a Reader with large enough
// size, it returns the underlying Sweeper.
func NewSweeperSize(rd io.Reader, size int) *Sweeper {
	// Is it already a Reader?
	s, ok := rd.(*Sweeper)
	if ok && len(s.buf) >= size {
		return s
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
func (s *Sweeper) Size() int { return len(s.buf) }

// Buffered returns the number of bytes that can be read from the current buffer.
func (s *Sweeper) Buffered() int { return s.w - s.r }

// Reset discards any buffered data, resets all state, and switches
// the buffered reader to read from r.
func (s *Sweeper) Reset(r io.Reader) {
	s.reset(s.buf, r)
}

func (s *Sweeper) reset(buf []byte, r io.Reader) {
	*s = Sweeper{
		buf: buf,
		rd:  r,
	}
}

func (s *Sweeper) readErr() error {
	err := s.err
	s.err = nil
	return err
}

// isZero is a helper function to find if a byte slice is all zeroes.
func (s *Sweeper) isBufZero() bool {
	for _, v := range s.buf {
		if v != 0 {
			return false
		}
	}
	return true
}

func (s *Sweeper) fill() {
	s.buf = append(s.buf, make([]byte, 1)...)

	// if the read position is greater than zero then the delimiter was found.
	if s.r > 0 {
		// Since the delimiter was found we may reset the buffer back to its
		// original size to clean up.
		temp := s.buf[s.r:]
		s.buf = make([]byte, defaultBufSize)
		copy(s.buf, temp)

		// Just set the read and write positions to 0 so then it can scan
		// from the beginning of the slice when it begins again.
		s.w = len(temp) - 1
		s.r = 0
	}

	if s.w >= len(s.buf) {
		panic("bufio: tried to fill full buffer")
	}

	// Read new data: try a limited number of times.
	for i := maxConsecutiveEmptyReads; i > 0; i-- {
		// Reads the length of the data that's not part of the already
		// existing data that I appended earlier. This means that it will
		// search a total of one byte in this function call.
		n, err := s.rd.Read(s.buf[s.w:])
		if n < 0 {
			panic(errNegativeRead)
		}

		s.w += n
		if err != nil {
			s.err = err
			return
		}
		if n > 0 {
			return
		}
	}
	s.err = io.ErrNoProgress
}

// ReadSliceWithString reads until the first occurrence of the delimiter in the
// input, returning a slice pointing at the bytes in the buffer.
// Any bytes that is after the delimiter is saved for the next read.
// If ReadSliceWithString encounters an error before finding a delimiter,
// it returns all the data in the buffer and the error itself.
// Although if the error is for EOF it keeps running until there isn't any
// data left and is just zeroed out.
// ReadSlice returns err != nil if and only if line does not end in delim.
func (s *Sweeper) ReadSliceWithString(delim []byte) (line []byte, err error) {
	s.fill() // Fill the buffer with data

	for {
		// Search buffer.
		if i := bytes.Index(s.buf[s.r:], []byte(delim)); i >= 0 {
			line = s.buf[:i+len(delim)]
			s.r = i + len(delim)

			break
		}

		// Pending error?
		if s.err != nil && s.err != io.EOF {
			line = s.buf[s.r:s.w]
			s.r = s.w
			err = s.readErr()
			break
		}

		// Note: This function does not check the buffered size in comparison
		// to the length of the buffer, because s.r is always zero and never
		// incremented since we are rescanning all of the buffer all of the time.

		if s.err != io.EOF {
			s.fill()
		} else {
			if s.isBufZero() {
				line = s.buf
				s.r = s.w
				err = s.err
				break
			}

			s.buf = s.buf[s.r:]

			s.r = 0
		}
	}

	return
}

// Read reads data into p.
// It returns the number of bytes read into p.
// The bytes are taken from at most one Read on the underlying Reader,
// hence n may be less than len(p).
// To read exactly len(p) bytes, use io.ReadFull(b, p).
// At EOF, the count will be zero and err will be io.EOF.
func (s *Sweeper) Read(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		return 0, s.readErr()
	}
	if s.r == s.w {
		if s.err != nil {
			return 0, s.readErr()
		}
		if len(p) >= len(s.buf) {
			// Large read, empty buffer.
			// Read directly into p to avoid copy.
			n, s.err = s.rd.Read(p)
			if n < 0 {
				panic(errNegativeRead)
			}
			return n, s.readErr()
		}
		// One read.
		// Do not use s.fill, which will loop.
		s.r = 0
		s.w = 0
		n, s.err = s.rd.Read(s.buf)
		if n < 0 {
			panic(errNegativeRead)
		}
		if n == 0 {
			return 0, s.readErr()
		}
		s.w += n
	}

	// copy as much as we can
	n = copy(p, s.buf[s.r:s.w])
	s.r += n

	return n, nil
}
