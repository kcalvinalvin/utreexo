// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package common

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
)

const (
	// MaxVarIntPayload is the maximum payload size for a variable length integer.
	MaxVarIntPayload = 9

	// MessageHeaderSize is the number of bytes in a bitcoin message header.
	// Bitcoin network (magic) 4 bytes + command 12 bytes + payload length 4 bytes +
	// checksum 4 bytes.
	MessageHeaderSize = 24

	// CommandSize is the fixed size of all commands in the common bitcoin message
	// header.  Shorter commands must be zero padded.
	CommandSize = 12

	// MaxMessagePayload is the maximum bytes a message can be regardless of other
	// individual limits imposed by messages themselves.
	MaxMessagePayload = (1024 * 1024 * 32) // 32MB

)

var (
	// littleEndian is a convenience variable since binary.LittleEndian is
	// quite long.
	littleEndian = binary.LittleEndian

	// bigEndian is a convenience variable since binary.BigEndian is quite
	// long.
	bigEndian = binary.BigEndian
)

// binarySerializer is just a wrapper around a slice of bytes
type binarySerializer struct {
	buf []byte
}

// binarySerializerFree provides a free list of buffers to use for serializing and
// deserializing primitive integer values to and from io.Readers and io.Writers.
var binarySerializerFree = sync.Pool{
	New: func() interface{} { return new(binarySerializer) },
}

// newSerializer allocates a new binarySerializer struct or grabs a cached one
// from binarySerializerFree
func newSerializer() *binarySerializer {
	b := binarySerializerFree.Get().(*binarySerializer)

	if b.buf == nil {
		b.buf = make([]byte, 8)
	}

	return b
}

// free saves used binarySerializer structs in ppFree; avoids an allocation per invocation.
func (bs *binarySerializer) free() {
	bs.buf = bs.buf[:0]
	binarySerializerFree.Put(bs)
}

// Uint8 reads a single byte from the provided reader using a buffer from the
// free list and returns it as a uint8.
func (l *binarySerializer) Uint8(r io.Reader) (uint8, error) {
	buf := l.buf[:1]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return buf[0], nil
}

// Uint16 reads two bytes from the provided reader using a buffer from the
// free list, converts it to a number using the provided byte order, and returns
// the resulting uint16.
func (l *binarySerializer) Uint16(r io.Reader, byteOrder binary.ByteOrder) (uint16, error) {
	buf := l.buf[:2]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return byteOrder.Uint16(buf), nil
}

// Uint32 reads four bytes from the provided reader using a buffer from the
// free list, converts it to a number using the provided byte order, and returns
// the resulting uint32.
func (l *binarySerializer) Uint32(r io.Reader, byteOrder binary.ByteOrder) (uint32, error) {
	buf := l.buf[:4]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return byteOrder.Uint32(buf), nil
}

// Uint64 reads eight bytes from the provided reader using a buffer from the
// free list, converts it to a number using the provided byte order, and returns
// the resulting uint64.
func (l *binarySerializer) Uint64(r io.Reader, byteOrder binary.ByteOrder) (uint64, error) {
	buf := l.buf[:8]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return byteOrder.Uint64(buf), nil
}

// PutUint8 copies the provided uint8 into a buffer from the free list and
// writes the resulting byte to the given writer.
func (l *binarySerializer) PutUint8(w io.Writer, val uint8) error {
	buf := l.buf[:1]
	buf[0] = val
	_, err := w.Write(buf)
	return err
}

// PutUint16 serializes the provided uint16 using the given byte order into a
// buffer from the free list and writes the resulting two bytes to the given
// writer.
func (l *binarySerializer) PutUint16(w io.Writer, byteOrder binary.ByteOrder, val uint16) error {
	buf := l.buf[:2]
	byteOrder.PutUint16(buf, val)
	_, err := w.Write(buf)
	return err
}

// PutUint32 serializes the provided uint32 using the given byte order into a
// buffer from the free list and writes the resulting four bytes to the given
// writer.
func (l *binarySerializer) PutUint32(w io.Writer, byteOrder binary.ByteOrder, val uint32) error {
	buf := l.buf[:4]
	byteOrder.PutUint32(buf, val)
	_, err := w.Write(buf)
	return err
}

// PutUint64 serializes the provided uint64 using the given byte order into a
// buffer from the free list and writes the resulting eight bytes to the given
// writer.
func (l *binarySerializer) PutUint64(w io.Writer, byteOrder binary.ByteOrder, val uint64) error {
	buf := l.buf[:8]
	byteOrder.PutUint64(buf, val)
	_, err := w.Write(buf)
	return err
}

// errNonCanonicalVarInt is the common format string used for non-canonically
// encoded variable length integer errors.
var errNonCanonicalVarInt = "non-canonical varint %x - discriminant %x must " +
	"encode a value greater than %x"

// uint32Time represents a unix timestamp encoded with a uint32.  It is used as
// a way to signal the readElement function how to decode a timestamp into a Go
// time.Time since it is otherwise ambiguous.
type uint32Time time.Time

// int64Time represents a unix timestamp encoded with an int64.  It is used as
// a way to signal the readElement function how to decode a timestamp into a Go
// time.Time since it is otherwise ambiguous.
type int64Time time.Time

// readElement reads the next sequence of bytes from r using little endian
// depending on the concrete type of element pointed to.
func readElement(r io.Reader, element interface{}) error {
	// Attempt to read the element based on the concrete type via fast
	// type assertions first.
	switch e := element.(type) {
	case *int32:
		bs := newSerializer()
		rv, err := bs.Uint32(r, littleEndian)
		bs.free()
		if err != nil {
			return err
		}
		*e = int32(rv)
		return nil

	case *uint32:
		bs := newSerializer()
		rv, err := bs.Uint32(r, littleEndian)
		bs.free()
		if err != nil {
			return err
		}
		*e = rv
		return nil

	case *int64:
		bs := newSerializer()
		rv, err := bs.Uint64(r, littleEndian)
		bs.free()
		if err != nil {
			return err
		}
		*e = int64(rv)
		return nil

	case *uint64:
		bs := newSerializer()
		rv, err := bs.Uint64(r, littleEndian)
		bs.free()
		if err != nil {
			return err
		}
		*e = rv
		return nil

	case *bool:
		bs := newSerializer()
		rv, err := bs.Uint8(r)
		bs.free()
		if err != nil {
			return err
		}
		if rv == 0x00 {
			*e = false
		} else {
			*e = true
		}
		return nil

	// Unix timestamp encoded as a uint32.
	case *uint32Time:
		bs := newSerializer()
		rv, err := bs.Uint32(r, binary.LittleEndian)
		bs.free()
		if err != nil {
			return err
		}
		*e = uint32Time(time.Unix(int64(rv), 0))
		return nil

	// Unix timestamp encoded as an int64.
	case *int64Time:
		bs := newSerializer()
		rv, err := bs.Uint64(r, binary.LittleEndian)
		bs.free()
		if err != nil {
			return err
		}
		*e = int64Time(time.Unix(int64(rv), 0))
		return nil

	// Message header checksum.
	case *[4]byte:
		_, err := io.ReadFull(r, e[:])
		if err != nil {
			return err
		}
		return nil

	// Message header command.
	case *[CommandSize]uint8:
		_, err := io.ReadFull(r, e[:])
		if err != nil {
			return err
		}
		return nil

	// IP address.
	case *[16]byte:
		_, err := io.ReadFull(r, e[:])
		if err != nil {
			return err
		}
		return nil

	case *chainhash.Hash:
		_, err := io.ReadFull(r, e[:])
		if err != nil {
			return err
		}
		return nil

		//case *ServiceFlag:
		//	bs := newSerializer()
		//	rv, err := bs.Uint64(r, littleEndian)
		//	bs.free()
		//	if err != nil {
		//		return err
		//	}
		//	*e = ServiceFlag(rv)
		//	return nil

		//case *InvType:
		//	bs := newSerializer()
		//	rv, err := bs.Uint32(r, littleEndian)
		//	bs.free()
		//	if err != nil {
		//		return err
		//	}
		//	*e = InvType(rv)
		//	return nil

		//case *BitcoinNet:
		//	bs := newSerializer()
		//	rv, err := bs.Uint32(r, littleEndian)
		//	bs.free()
		//	if err != nil {
		//		return err
		//	}
		//	*e = BitcoinNet(rv)
		//	return nil

		//case *BloomUpdateType:
		//	bs := newSerializer()
		//	rv, err := bs.Uint8(r)
		//	bs.free()
		//	if err != nil {
		//		return err
		//	}
		//	*e = BloomUpdateType(rv)
		//	return nil

		//case *RejectCode:
		//	bs := newSerializer()
		//	rv, err := bs.Uint8(r)
		//	bs.free()
		//	if err != nil {
		//		return err
		//	}
		//	*e = RejectCode(rv)
		//	return nil
	}

	// Fall back to the slower binary.Read if a fast path was not available
	// above.
	return binary.Read(r, littleEndian, element)
}

// readElements reads multiple items from r.  It is equivalent to multiple
// calls to readElement.
func readElements(r io.Reader, elements ...interface{}) error {
	for _, element := range elements {
		err := readElement(r, element)
		if err != nil {
			return err
		}
	}
	return nil
}

// writeElement writes the little endian representation of element to w.
func writeElement(w io.Writer, element interface{}) error {
	// Attempt to write the element based on the concrete type via fast
	// type assertions first.
	switch e := element.(type) {
	case int32:
		bs := newSerializer()
		err := bs.PutUint32(w, littleEndian, uint32(e))
		bs.free()
		return err

	case uint32:
		bs := newSerializer()
		err := bs.PutUint32(w, littleEndian, e)
		bs.free()
		return err

	case int64:
		bs := newSerializer()
		err := bs.PutUint64(w, littleEndian, uint64(e))
		bs.free()
		return err

	case uint64:
		bs := newSerializer()
		err := bs.PutUint64(w, littleEndian, e)
		bs.free()
		return err

	case bool:
		var err error
		if e {
			bs := newSerializer()
			err = bs.PutUint8(w, 0x01)
			bs.free()
			if err != nil {
				return err
			}
		} else {
			bs := newSerializer()
			err = bs.PutUint8(w, 0x00)
			bs.free()
			if err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
		return nil

	// Message header checksum.
	case [4]byte:
		_, err := w.Write(e[:])
		if err != nil {
			return err
		}
		return nil

	// Message header command.
	case [CommandSize]uint8:
		_, err := w.Write(e[:])
		if err != nil {
			return err
		}
		return nil

	// IP address.
	case [16]byte:
		_, err := w.Write(e[:])
		if err != nil {
			return err
		}
		return nil

	case *chainhash.Hash:
		_, err := w.Write(e[:])
		if err != nil {
			return err
		}
		return nil

		//case ServiceFlag:
		//	bs := newSerializer()
		//	err := bs.PutUint64(w, littleEndian, uint64(e))
		//	bs.free()
		//	return err

		//case InvType:
		//	bs := newSerializer()
		//	err := bs.PutUint32(w, littleEndian, uint32(e))
		//	bs.free()
		//	return err

		//case BitcoinNet:
		//	bs := newSerializer()
		//	err := bs.PutUint32(w, littleEndian, uint32(e))
		//	bs.free()
		//	return err

		//case BloomUpdateType:
		//	bs := newSerializer()
		//	err := bs.PutUint8(w, uint8(e))
		//	bs.free()
		//	return err

		//case RejectCode:
		//	bs := newSerializer()
		//	err := bs.PutUint8(w, uint8(e))
		//	bs.free()
		//	return err
	}

	// Fall back to the slower binary.Write if a fast path was not available
	// above.
	return binary.Write(w, littleEndian, element)
}

// writeElements writes multiple items to w.  It is equivalent to multiple
// calls to writeElement.
func writeElements(w io.Writer, elements ...interface{}) error {
	for _, element := range elements {
		err := writeElement(w, element)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadVarInt reads a variable length integer from r and returns it as a uint64.
func ReadVarInt(r io.Reader, pver uint32) (uint64, error) {
	bs := newSerializer()
	discriminant, err := bs.Uint8(r)
	bs.free()
	if err != nil {
		return 0, err
	}

	var rv uint64
	switch discriminant {
	case 0xff:
		bs := newSerializer()
		sv, err := bs.Uint64(r, littleEndian)
		bs.free()
		if err != nil {
			return 0, err
		}
		rv = sv

		// The encoding is not canonical if the value could have been
		// encoded using fewer bytes.
		min := uint64(0x100000000)
		if rv < min {
			return 0, messageError("ReadVarInt", fmt.Sprintf(
				errNonCanonicalVarInt, rv, discriminant, min))
		}

	case 0xfe:
		bs := newSerializer()
		sv, err := bs.Uint32(r, littleEndian)
		bs.free()
		if err != nil {
			return 0, err
		}
		rv = uint64(sv)

		// The encoding is not canonical if the value could have been
		// encoded using fewer bytes.
		min := uint64(0x10000)
		if rv < min {
			return 0, messageError("ReadVarInt", fmt.Sprintf(
				errNonCanonicalVarInt, rv, discriminant, min))
		}

	case 0xfd:
		bs := newSerializer()
		sv, err := bs.Uint16(r, littleEndian)
		bs.free()
		if err != nil {
			return 0, err
		}
		rv = uint64(sv)

		// The encoding is not canonical if the value could have been
		// encoded using fewer bytes.
		min := uint64(0xfd)
		if rv < min {
			return 0, messageError("ReadVarInt", fmt.Sprintf(
				errNonCanonicalVarInt, rv, discriminant, min))
		}

	default:
		rv = uint64(discriminant)
	}

	return rv, nil
}

// WriteVarInt serializes val to w using a variable number of bytes depending
// on its value.
func WriteVarInt(w io.Writer, pver uint32, val uint64) error {
	if val < 0xfd {
		bs := newSerializer()
		err := bs.PutUint8(w, uint8(val))
		bs.free()
		return err
	}

	if val <= math.MaxUint16 {
		bs := newSerializer()
		err := bs.PutUint8(w, 0xfd)
		if err != nil {
			bs.free()
			return err
		}

		err = bs.PutUint16(w, littleEndian, uint16(val))
		bs.free()
		return err
	}

	if val <= math.MaxUint32 {
		bs := newSerializer()
		err := bs.PutUint8(w, 0xfe)
		if err != nil {
			bs.free()
			return err
		}
		err = bs.PutUint32(w, littleEndian, uint32(val))
		bs.free()
		return err
	}

	bs := newSerializer()
	err := bs.PutUint8(w, 0xff)
	if err != nil {
		bs.free()
		return err
	}
	err = bs.PutUint64(w, littleEndian, val)
	bs.free()
	return err
}

// VarIntSerializeSize returns the number of bytes it would take to serialize
// val as a variable length integer.
func VarIntSerializeSize(val uint64) int {
	// The value is small enough to be represented by itself, so it's
	// just 1 byte.
	if val < 0xfd {
		return 1
	}

	// Discriminant 1 byte plus 2 bytes for the uint16.
	if val <= math.MaxUint16 {
		return 3
	}

	// Discriminant 1 byte plus 4 bytes for the uint32.
	if val <= math.MaxUint32 {
		return 5
	}

	// Discriminant 1 byte plus 8 bytes for the uint64.
	return 9
}

// ReadVarString reads a variable length string from r and returns it as a Go
// string.  A variable length string is encoded as a variable length integer
// containing the length of the string followed by the bytes that represent the
// string itself.  An error is returned if the length is greater than the
// maximum block payload size since it helps protect against memory exhaustion
// attacks and forced panics through malformed messages.
func ReadVarString(r io.Reader, pver uint32) (string, error) {
	count, err := ReadVarInt(r, pver)
	if err != nil {
		return "", err
	}

	// Prevent variable length strings that are larger than the maximum
	// message size.  It would be possible to cause memory exhaustion and
	// panics without a sane upper bound on this count.
	if count > MaxMessagePayload {
		str := fmt.Sprintf("variable length string is too long "+
			"[count %d, max %d]", count, MaxMessagePayload)
		return "", messageError("ReadVarString", str)
	}

	buf := make([]byte, count)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

// WriteVarString serializes str to w as a variable length integer containing
// the length of the string followed by the bytes that represent the string
// itself.
func WriteVarString(w io.Writer, pver uint32, str string) error {
	err := WriteVarInt(w, pver, uint64(len(str)))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(str))
	return err
}

// ReadVarBytes reads a variable length byte array.  A byte array is encoded
// as a varInt containing the length of the array followed by the bytes
// themselves.  An error is returned if the length is greater than the
// passed maxAllowed parameter which helps protect against memory exhaustion
// attacks and forced panics through malformed messages.  The fieldName
// parameter is only used for the error message so it provides more context in
// the error.
func ReadVarBytes(r io.Reader, pver uint32, maxAllowed uint32,
	fieldName string) ([]byte, error) {

	count, err := ReadVarInt(r, pver)
	if err != nil {
		return nil, err
	}

	// Prevent byte array larger than the max message size.  It would
	// be possible to cause memory exhaustion and panics without a sane
	// upper bound on this count.
	if count > uint64(maxAllowed) {
		str := fmt.Sprintf("%s is larger than the max allowed size "+
			"[count %d, max %d]", fieldName, count, maxAllowed)
		return nil, messageError("ReadVarBytes", str)
	}

	b := make([]byte, count)
	_, err = io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// WriteVarBytes serializes a variable length byte array to w as a varInt
// containing the number of bytes, followed by the bytes themselves.
func WriteVarBytes(w io.Writer, pver uint32, bytes []byte) error {
	slen := uint64(len(bytes))
	err := WriteVarInt(w, pver, slen)
	if err != nil {
		return err
	}

	_, err = w.Write(bytes)
	return err
}

// randomUint64 returns a cryptographically random uint64 value.  This
// unexported version takes a reader primarily to ensure the error paths
// can be properly tested by passing a fake reader in the tests.
func randomUint64(r io.Reader) (uint64, error) {
	bs := newSerializer()
	rv, err := bs.Uint64(r, bigEndian)
	bs.free()
	if err != nil {
		return 0, err
	}
	return rv, nil
}

// RandomUint64 returns a cryptographically random uint64 value.
func RandomUint64() (uint64, error) {
	return randomUint64(rand.Reader)
}

// FreeBytes is a wrapper around bytes
type FreeBytes struct {
	Bytes []byte
}

// FreeBytes the bytes to the FreeBytes pool
func (fb *FreeBytes) Free() {
	fb.Bytes = fb.Bytes[:0]
	FreeBytesPool.Put(fb)
}

// NewFreeBytes returns a parentHashBytes from the pool. Will allocate if the
// Pool returns parentHashBytes that doesn't have bytes allocated
func NewFreeBytes() *FreeBytes {
	fb := FreeBytesPool.Get().(*FreeBytes)

	if fb.Bytes == nil {
		// set minimum to 64 since that's what parentHash()
		// requires and parentHash() is called very frequently
		fb.Bytes = make([]byte, 0, 64)
	}

	return fb
}

// FreeBytesPool is the pool of bytes to recycle&relieve gc pressure.
var FreeBytesPool = sync.Pool{
	New: func() interface{} { return new(FreeBytes) },
}

// Uint8 reads a single byte from the provided reader using a buffer from the
// free list and returns it as a uint8.
func (fb *FreeBytes) Uint8(r io.Reader) (uint8, error) {
	buf := fb.Bytes[:1]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return buf[0], nil
}

// Uint16 reads two bytes from the provided reader using a buffer from the
// free list, converts it to a number using the provided byte order, and returns
// the resulting uint16.
func (fb *FreeBytes) Uint16(r io.Reader, byteOrder binary.ByteOrder) (uint16, error) {
	buf := fb.Bytes[:2]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return byteOrder.Uint16(buf), nil
}

// Uint32 reads four bytes from the provided reader using a buffer from the
// free list, converts it to a number using the provided byte order, and returns
// the resulting uint32.
func (fb *FreeBytes) Uint32(r io.Reader, byteOrder binary.ByteOrder) (uint32, error) {
	buf := fb.Bytes[:4]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return byteOrder.Uint32(buf), nil
}

// Uint64 reads eight bytes from the provided reader using a buffer from the
// free list, converts it to a number using the provided byte order, and returns
// the resulting uint64.
func (fb *FreeBytes) Uint64(r io.Reader, byteOrder binary.ByteOrder) (uint64, error) {
	buf := fb.Bytes[:8]
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return byteOrder.Uint64(buf), nil
}

// PutUint8 copies the provided uint8 into a buffer from the free list and
// writes the resulting byte to the given writer.
func (fb *FreeBytes) PutUint8(w io.Writer, val uint8) error {
	buf := fb.Bytes[:1]
	buf[0] = val
	_, err := w.Write(buf)
	return err
}

// PutUint16 serializes the provided uint16 using the given byte order into a
// buffer from the free list and writes the resulting two bytes to the given
// writer.
func (fb *FreeBytes) PutUint16(w io.Writer, byteOrder binary.ByteOrder, val uint16) error {
	buf := fb.Bytes[:2]
	byteOrder.PutUint16(buf, val)
	_, err := w.Write(buf)
	return err
}

// PutUint32 serializes the provided uint32 using the given byte order into a
// buffer from the free list and writes the resulting four bytes to the given
// writer.
func (fb *FreeBytes) PutUint32(w io.Writer, byteOrder binary.ByteOrder, val uint32) error {
	buf := fb.Bytes[:4]
	byteOrder.PutUint32(buf, val)
	_, err := w.Write(buf)
	return err
}

// PutUint64 serializes the provided uint64 using the given byte order into a
// buffer from the free list and writes the resulting eight bytes to the given
// writer.
func (fb *FreeBytes) PutUint64(w io.Writer, byteOrder binary.ByteOrder, val uint64) error {
	buf := fb.Bytes[:8]
	byteOrder.PutUint64(buf, val)
	_, err := w.Write(buf)
	return err
}