package skytable

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/satvik007/skytable-go/internal"
	"github.com/satvik007/skytable-go/internal/proto"
	"github.com/satvik007/skytable-go/internal/util"
)

type Cmder interface {
	Name() string
	FullName() string
	Args() []interface{}
	String() string
	stringArg(int) string
	firstKeyPos() int8
	SetFirstKeyPos(int8)

	readTimeout() *time.Duration
	readReply(rd *proto.Reader) error

	SetErr(error)
	Err() error
}

func setCmdsErr(cmds []Cmder, e error) {
	for _, cmd := range cmds {
		if cmd.Err() == nil {
			cmd.SetErr(e)
		}
	}
}

func cmdsFirstErr(cmds []Cmder) error {
	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			return err
		}
	}
	return nil
}

func writeCmds(wr *proto.Writer, cmds []Cmder) error {
	if err := wr.WriteMetaFrame(len(cmds)); err != nil {
		return err
	}
	for _, cmd := range cmds {
		if err := writeCmd(wr, cmd); err != nil {
			return err
		}
	}
	return nil
}

func writeCmd(wr *proto.Writer, cmd Cmder) error {
	return wr.WriteArgs(cmd.Args())
}

func cmdString(cmd Cmder, val interface{}) string {
	b := make([]byte, 0, 64)

	for i, arg := range cmd.Args() {
		if i > 0 {
			b = append(b, ' ')
		}
		b = internal.AppendArg(b, arg)
	}

	if err := cmd.Err(); err != nil {
		b = append(b, ": "...)
		b = append(b, err.Error()...)
	} else if val != nil {
		b = append(b, ": "...)
		b = internal.AppendArg(b, val)
	}

	return internal.String(b)
}

// ------------------------------------------------------------------------------

type baseCmd struct {
	ctx    context.Context
	args   []interface{}
	err    error
	keyPos int8

	_readTimeout *time.Duration
}

var _ Cmder = (*Cmd)(nil)

func (cmd *baseCmd) Name() string {
	if len(cmd.args) == 0 {
		return ""
	}
	// Cmd name must be lower cased.
	return internal.ToLower(cmd.stringArg(0))
}

func (cmd *baseCmd) FullName() string {
	switch name := cmd.Name(); name {
	case "command":
		if len(cmd.args) == 1 {
			return name
		}
		if s2, ok := cmd.args[1].(string); ok {
			return name + " " + s2
		}
		return name
	default:
		return name
	}
}

func (cmd *baseCmd) Args() []interface{} {
	return cmd.args
}

func (cmd *baseCmd) stringArg(pos int) string {
	if pos < 0 || pos >= len(cmd.args) {
		return ""
	}
	arg := cmd.args[pos]
	switch v := arg.(type) {
	case string:
		return v
	default:
		// TODO: consider using appendArg
		return fmt.Sprint(v)
	}
}

func (cmd *baseCmd) firstKeyPos() int8 {
	return cmd.keyPos
}

func (cmd *baseCmd) SetFirstKeyPos(keyPos int8) {
	cmd.keyPos = keyPos
}

func (cmd *baseCmd) SetErr(e error) {
	cmd.err = e
}

func (cmd *baseCmd) Err() error {
	return cmd.err
}

func (cmd *baseCmd) readTimeout() *time.Duration {
	return cmd._readTimeout
}

// nolint
func (cmd *baseCmd) setReadTimeout(d time.Duration) {
	cmd._readTimeout = &d
}

// ------------------------------------------------------------------------------

type Cmd struct {
	baseCmd

	val interface{}
}

func NewCmd(ctx context.Context, args ...interface{}) *Cmd {
	return &Cmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *Cmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *Cmd) SetVal(val interface{}) {
	cmd.val = val
}

func (cmd *Cmd) Val() interface{} {
	return cmd.val
}

func (cmd *Cmd) Result() (interface{}, error) {
	return cmd.val, cmd.err
}

func (cmd *Cmd) Text() (string, error) {
	if cmd.err != nil {
		return "", cmd.err
	}
	return toString(cmd.val)
}

func toString(val interface{}) (string, error) {
	switch val := val.(type) {
	case string:
		return val, nil
	default:
		err := fmt.Errorf("skytable: unexpected type=%T for String", val)
		return "", err
	}
}

func (cmd *Cmd) Int() (int, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	switch val := cmd.val.(type) {
	case int64:
		return int(val), nil
	case string:
		return strconv.Atoi(val)
	default:
		err := fmt.Errorf("skytable: unexpected type=%T for Int", val)
		return 0, err
	}
}

func (cmd *Cmd) Int64() (int64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return toInt64(cmd.val)
}

func toInt64(val interface{}) (int64, error) {
	switch val := val.(type) {
	case int64:
		return val, nil
	case string:
		return strconv.ParseInt(val, 10, 64)
	default:
		err := fmt.Errorf("skytable: unexpected type=%T for Int64", val)
		return 0, err
	}
}

func (cmd *Cmd) Uint64() (uint64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return toUint64(cmd.val)
}

func toUint64(val interface{}) (uint64, error) {
	switch val := val.(type) {
	case int64:
		return uint64(val), nil
	case string:
		return strconv.ParseUint(val, 10, 64)
	default:
		err := fmt.Errorf("skytable: unexpected type=%T for Uint64", val)
		return 0, err
	}
}

func (cmd *Cmd) Float32() (float32, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return toFloat32(cmd.val)
}

func toFloat32(val interface{}) (float32, error) {
	switch val := val.(type) {
	case int64:
		return float32(val), nil
	case string:
		f, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return 0, err
		}
		return float32(f), nil
	default:
		err := fmt.Errorf("skytable: unexpected type=%T for Float32", val)
		return 0, err
	}
}

func (cmd *Cmd) Float64() (float64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return toFloat64(cmd.val)
}

func toFloat64(val interface{}) (float64, error) {
	switch val := val.(type) {
	case int64:
		return float64(val), nil
	case string:
		return strconv.ParseFloat(val, 64)
	default:
		err := fmt.Errorf("skytable: unexpected type=%T for Float64", val)
		return 0, err
	}
}

func (cmd *Cmd) Bool() (bool, error) {
	if cmd.err != nil {
		return false, cmd.err
	}
	return toBool(cmd.val)
}

func toBool(val interface{}) (bool, error) {
	switch val := val.(type) {
	case int64:
		return val != 0, nil
	case string:
		return strconv.ParseBool(val)
	default:
		err := fmt.Errorf("skytable: unexpected type=%T for Bool", val)
		return false, err
	}
}

func (cmd *Cmd) Slice() ([]interface{}, error) {
	if cmd.err != nil {
		return nil, cmd.err
	}
	switch val := cmd.val.(type) {
	case []interface{}:
		return val, nil
	default:
		return nil, fmt.Errorf("skytable: unexpected type=%T for Slice", val)
	}
}

func (cmd *Cmd) StringSlice() ([]string, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	ss := make([]string, len(slice))
	for i, iface := range slice {
		val, err := toString(iface)
		if err != nil {
			return nil, err
		}
		ss[i] = val
	}
	return ss, nil
}

func (cmd *Cmd) Int64Slice() ([]int64, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	nums := make([]int64, len(slice))
	for i, iface := range slice {
		val, err := toInt64(iface)
		if err != nil {
			return nil, err
		}
		nums[i] = val
	}
	return nums, nil
}

func (cmd *Cmd) Uint64Slice() ([]uint64, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	nums := make([]uint64, len(slice))
	for i, iface := range slice {
		val, err := toUint64(iface)
		if err != nil {
			return nil, err
		}
		nums[i] = val
	}
	return nums, nil
}

func (cmd *Cmd) Float32Slice() ([]float32, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	floats := make([]float32, len(slice))
	for i, iface := range slice {
		val, err := toFloat32(iface)
		if err != nil {
			return nil, err
		}
		floats[i] = val
	}
	return floats, nil
}

func (cmd *Cmd) Float64Slice() ([]float64, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	floats := make([]float64, len(slice))
	for i, iface := range slice {
		val, err := toFloat64(iface)
		if err != nil {
			return nil, err
		}
		floats[i] = val
	}
	return floats, nil
}

func (cmd *Cmd) BoolSlice() ([]bool, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	bools := make([]bool, len(slice))
	for i, iface := range slice {
		val, err := toBool(iface)
		if err != nil {
			return nil, err
		}
		bools[i] = val
	}
	return bools, nil
}

func (cmd *Cmd) readReply(rd *proto.Reader) (err error) {
	cmd.val, err = rd.ReadReply()
	return err
}

//------------------------------------------------------------------------------

type IntCmd struct {
	baseCmd

	val int64
}

var _ Cmder = (*IntCmd)(nil)

func NewIntCmd(ctx context.Context, args ...interface{}) *IntCmd {
	return &IntCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *IntCmd) SetVal(val int64) {
	cmd.val = val
}

func (cmd *IntCmd) Val() int64 {
	return cmd.val
}

func (cmd *IntCmd) Result() (int64, error) {
	return cmd.val, cmd.err
}

func (cmd *IntCmd) Uint64() (uint64, error) {
	return uint64(cmd.val), cmd.err
}

func (cmd *IntCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *IntCmd) readReply(rd *proto.Reader) (err error) {
	cmd.val, err = rd.ReadInt()
	return err
}

// ------------------------------------------------------------------------------

type SliceCmd struct {
	baseCmd

	val []interface{}
}

var _ Cmder = (*SliceCmd)(nil)

func NewSliceCmd(ctx context.Context, args ...interface{}) *SliceCmd {
	return &SliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *SliceCmd) SetVal(val []interface{}) {
	cmd.val = val
}

func (cmd *SliceCmd) Val() []interface{} {
	return cmd.val
}

func (cmd *SliceCmd) Result() ([]interface{}, error) {
	return cmd.val, cmd.err
}

func (cmd *SliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *SliceCmd) readReply(rd *proto.Reader) (err error) {
	cmd.val, err = rd.ReadSlice()
	return err
}

// ------------------------------------------------------------------------------

type StatusCmd struct {
	baseCmd

	val int64
}

var _ Cmder = (*StatusCmd)(nil)

func NewStatusCmd(ctx context.Context, args ...interface{}) *StatusCmd {
	return &StatusCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *StatusCmd) SetVal(val int64) {
	cmd.val = val
}

func (cmd *StatusCmd) Val() int64 {
	return cmd.val
}

func (cmd *StatusCmd) Result() (int64, error) {
	return cmd.val, cmd.err
}

func (cmd *StatusCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *StatusCmd) readReply(rd *proto.Reader) (err error) {
	cmd.val, err = rd.ReadStatus()
	return err
}

// ------------------------------------------------------------------------------

type StringCmd struct {
	baseCmd

	val string
}

var _ Cmder = (*StringCmd)(nil)

func NewStringCmd(ctx context.Context, args ...interface{}) *StringCmd {
	return &StringCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *StringCmd) SetVal(val string) {
	cmd.val = val
}

func (cmd *StringCmd) Val() string {
	return cmd.val
}

func (cmd *StringCmd) Result() (string, error) {
	return cmd.Val(), cmd.err
}

func (cmd *StringCmd) Bytes() ([]byte, error) {
	return util.StringToBytes(cmd.val), cmd.err
}

func (cmd *StringCmd) Bool() (bool, error) {
	if cmd.err != nil {
		return false, cmd.err
	}
	return strconv.ParseBool(cmd.val)
}

func (cmd *StringCmd) Int() (int, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.Atoi(cmd.Val())
}

func (cmd *StringCmd) Int64() (int64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.ParseInt(cmd.Val(), 10, 64)
}

func (cmd *StringCmd) Uint64() (uint64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.ParseUint(cmd.Val(), 10, 64)
}

func (cmd *StringCmd) Float32() (float32, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	f, err := strconv.ParseFloat(cmd.Val(), 32)
	if err != nil {
		return 0, err
	}
	return float32(f), nil
}

func (cmd *StringCmd) Float64() (float64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.ParseFloat(cmd.Val(), 64)
}

func (cmd *StringCmd) Time() (time.Time, error) {
	if cmd.err != nil {
		return time.Time{}, cmd.err
	}
	return time.Parse(time.RFC3339Nano, cmd.Val())
}

func (cmd *StringCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *StringCmd) readReply(rd *proto.Reader) (err error) {
	cmd.val, err = rd.ReadString()
	return err
}

//------------------------------------------------------------------------------

type StringSliceCmd struct {
	baseCmd

	val []string
}

var _ Cmder = (*StringSliceCmd)(nil)

func NewStringSliceCmd(ctx context.Context, args ...interface{}) *StringSliceCmd {
	return &StringSliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *StringSliceCmd) SetVal(val []string) {
	cmd.val = val
}

func (cmd *StringSliceCmd) Val() []string {
	return cmd.val
}

func (cmd *StringSliceCmd) Result() ([]string, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *StringSliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *StringSliceCmd) readReply(rd *proto.Reader) error {
	n, err := rd.ReadArrayLen()
	if err != nil {
		return err
	}
	cmd.val = make([]string, n)
	for i := 0; i < len(cmd.val); i++ {
		switch s, err := rd.ReadString(); {
		case err == Nil:
			cmd.val[i] = ""
		case err != nil:
			return err
		default:
			cmd.val[i] = s
		}
	}
	return nil
}
