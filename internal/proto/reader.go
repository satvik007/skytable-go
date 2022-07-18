package proto

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strconv"

	//nolint:go-cyclo
	"github.com/satvik007/skytable-go/internal/util"
)

const Nil = SkytableError("skytable: nil")
const OverwriteError = SkytableError("skytable: overwrite error")
const ActionError = SkytableError("skytable: action error")
const PacketError = SkytableError("skytable: packet error")
const ServerError = SkytableError("skytable: server error")
const OtherError = SkytableError("skytable: other error")
const WrongTypeError = SkytableError("skytable: wrong type error")
const UnknownDataTypeError = SkytableError("skytable: unknown data type")
const EncodingError = SkytableError("skytable: encoding error")
const BadCredentials = SkytableError("skytable: bad credentials")
const AuthnRealmError = SkytableError("skytable: authn realm error")

var CodeToErrorMap = map[int64]SkytableError{
	1:  Nil,
	2:  OverwriteError,
	3:  ActionError,
	4:  PacketError,
	5:  ServerError,
	6:  OtherError,
	7:  WrongTypeError,
	8:  UnknownDataTypeError,
	9:  EncodingError,
	10: BadCredentials,
	11: AuthnRealmError,
}

const (
	RespString    = '+' // +<length>\n<bytes>\n
	RespArray     = '&' // &<c>\n<elements>
	RespAnyArray  = '~' // ~<c>\n<elements>
	RespInt       = ':' // :<length>\n<number>
	RespFloat     = '%' // %<length>\n<bytes>\n
	RespBlob      = '?' // ?<length>\n<bytes>
	RespStatus    = '!' // !<length>\n<statusCode>\n
	RespMetaFrame = '*' // *<number>\n
)

type SkytableError string

func (e SkytableError) Error() string { return string(e) }

func (SkytableError) SkytableError() {}

func ParseErrorReply(line []byte) error {
	return SkytableError(line[1:])
}

// ------------------------------------------------------------------------------

type Reader struct {
	rd *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd: bufio.NewReader(rd),
	}
}

func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

func (r *Reader) Peek(n int) ([]byte, error) {
	return r.rd.Peek(n)
}

func (r *Reader) Reset(rd io.Reader) {
	r.rd.Reset(rd)
}

// PeekReplyType returns the data type of the next response without advancing the Reader,
// and discard the attribute type.
func (r *Reader) PeekReplyType() (byte, error) {
	b, err := r.rd.Peek(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

// ReadLine Return a valid reply, it will check the protocol or skytable error,
// and discard the attribute type.
func (r *Reader) ReadLine() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	return line, nil
}

// readLine returns an error if:
//   - there is a pending read error;
//   - or line does not end with \r\n.
func (r *Reader) readLine() ([]byte, error) {
	b, err := r.rd.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return nil, err
		}

		full := make([]byte, len(b))
		copy(full, b)

		b, err = r.rd.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		full = append(full, b...) //nolint:makezero
		b = full
	}
	if len(b) <= 1 || b[len(b)-1] != '\n' {
		return nil, fmt.Errorf("skytable: invalid reply: %q", b)
	}
	return b[:len(b)-1], nil
}

func (r *Reader) readInt() (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	return util.ParseInt(line[1:], 10, 64)
}

func (r *Reader) readFloat() (float32, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	v := string(line[1:])
	switch string(line[1:]) {
	case "inf":
		return float32(math.Inf(1)), nil
	case "-inf":
		return float32(math.Inf(-1)), nil
	}
	val, err := strconv.ParseFloat(v, 32)
	return float32(val), err
}

func (r *Reader) readString(line []byte) (string, error) {
	n, err := replyLen(line)
	if err != nil {
		return "", err
	}

	b := make([]byte, n+1)
	_, err = io.ReadFull(r.rd, b)
	if err != nil {
		return "", err
	}

	return util.BytesToString(b[:n]), nil
}

func (r *Reader) readSlice(line []byte) ([]interface{}, error) {
	n, err := replyLen(line)
	if err != nil {
		return nil, err
	}

	val := make([]interface{}, n)
	for i := 0; i < len(val); i++ {
		v, err := r.ReadReply()
		if err != nil {
			if err == Nil {
				val[i] = nil
				continue
			}
			if err, ok := err.(SkytableError); ok {
				val[i] = err
				continue
			}
			return nil, err
		}
		val[i] = v
	}
	return val, nil
}

func (r *Reader) readStatus(line []byte) (int64, error) {
	_, err := util.Atoi(line[1:])
	if err != nil {
		return 0, err
	}
	line, err = r.readLine()
	if err != nil {
		return 0, err
	}
	val, err := util.Atoi(line)
	if err != nil {
		return 0, fmt.Errorf("skytable: %.100q", line)
	}
	if val == 0 {
		return 0, nil
	} else if val == 1 {
		return 0, Nil
	} else if val < 12 {
		return 0, CodeToErrorMap[int64(val)]
	} else {
		return 0, fmt.Errorf("skytable: unknown error occured, message: %v", val)
	}
}

func replyLen(line []byte) (n int, err error) {
	n, err = util.Atoi(line[1:])
	if err != nil {
		return 0, err
	}

	if n < 0 {
		return 0, fmt.Errorf("skytable: invalid reply: %q", line)
	}

	return n, nil
}

// -------------------------------

func (r *Reader) ReadInt() (int64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespStatus:
		if _, err := r.readStatus(line); err != nil {
			return 0, err
		}
	case RespInt:
		return r.readInt()
	}
	return 0, fmt.Errorf("skytable: can't parse int reply: %.100q", line)
}

func (r *Reader) ReadFloat() (float32, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespStatus:
		if _, err := r.readStatus(line); err != nil {
			return 0, err
		}
	case RespFloat:
		return r.readFloat()
	}
	return 0, fmt.Errorf("skytable: can't parse float reply: %.100q", line)
}

func (r *Reader) ReadString() (string, error) {
	line, err := r.ReadLine()
	if err != nil {
		return "", err
	}

	switch line[0] {
	case RespStatus:
		if _, err := r.readStatus(line); err != nil {
			return "", err
		}
	case RespString, RespBlob:
		return r.readString(line)
	}
	return "", fmt.Errorf("skytable: can't parse reply=%.100q reading string", line)
}

func (r *Reader) ReadSlice() ([]interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case RespStatus:
		if _, err := r.readStatus(line); err != nil {
			return nil, err
		}
	case RespArray:
		return r.readSlice(line)
	}
	return nil, fmt.Errorf("skytable: can't parse reply=%.100q reading slice", line)
}

func (r *Reader) ReadStatus() (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	if line[0] != RespStatus {
		return 0, fmt.Errorf("skytable: can't parse reply=%.100q reading slice", line)
	}
	return r.readStatus(line)
}

func (r *Reader) ReadReply() (interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case RespStatus:
		return r.readStatus(line)
	case RespInt:
		return r.readInt()
	case RespFloat:
		return r.readFloat()
	case RespString:
		return r.readString(line)
	case RespBlob:
		return r.readLine()
	case RespArray:
		return r.readSlice(line)
	}
	return nil, fmt.Errorf("skytable: can't parse %.100q", line)
}

func (r *Reader) ReadMetaFrame() (int, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	if line[0] != RespMetaFrame {
		return 0, fmt.Errorf("skytable: invalid meta frame: %q", line)
	}
	return util.Atoi(line[1:])
}

func (r *Reader) ReadBytes() ([]byte, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case RespStatus:
		if _, err := r.readStatus(line); err != nil {
			return nil, err
		}
	case RespBlob:
		return r.readLine()
	}
	return nil, fmt.Errorf("skytable: can't parse %.100q", line)
}
