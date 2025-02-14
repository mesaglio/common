package detach

import (
	"context"
	"errors"
	"io"
)

// ErrDetach indicates that an attach session was manually detached by
// the user.
var ErrDetach = errors.New("detached from container")

// Copy es similar a io.Copy pero admite una secuencia de teclas de separación para salir y utiliza un contexto para cancelación.
func Copy(ctx context.Context, dst io.Writer, src io.Reader, keys []byte) (written int64, err error) {
	buf := make([]byte, 32*1024)
LOOP:
	for {
		select {
		case <-ctx.Done():
			// logrus.Debug("Copy operation canceled")
			return written, ctx.Err() // Salir si el contexto ha sido cancelado
		default:
			nr, er := src.Read(buf)
			if nr > 0 {
				preservBuf := []byte{}
				for i, key := range keys {
					preservBuf = append(preservBuf, buf[0:nr]...)
					if nr != 1 || buf[0] != key {
						break LOOP
					}
					if i == len(keys)-1 {
						return 0, ErrDetach
					}
					nr, er = src.Read(buf)
				}
				var nw int
				var ew error
				if len(preservBuf) > 0 {
					nw, ew = dst.Write(preservBuf)
					nr = len(preservBuf)
				} else {
					nw, ew = dst.Write(buf[0:nr])
				}
				if nw > 0 {
					written += int64(nw)
				}
				if ew != nil {
					err = ew
					break LOOP
				}
				if nr != nw {
					err = io.ErrShortWrite
					break LOOP
				}
			}
			if er != nil {
				if er != io.EOF {
					err = er
				}
				break LOOP
			}
		}
	}
	return written, err
}
