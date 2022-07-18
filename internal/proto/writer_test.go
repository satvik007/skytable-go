package proto_test

import (
	"bytes"
	"encoding"
	"fmt"
	"net"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/satvik007/skytable-go/internal/proto"
)

type MyType struct{}

var _ encoding.BinaryMarshaler = (*MyType)(nil)

func (t *MyType) MarshalBinary() ([]byte, error) {
	return []byte("hello"), nil
}

var _ = Describe("WriteBuffer", func() {
	var buf *bytes.Buffer
	var wr *proto.Writer

	BeforeEach(func() {
		buf = new(bytes.Buffer)
		wr = proto.NewWriter(buf)
	})

	It("should write args", func() {
		err := wr.WriteArgs([]interface{}{
			"string",
			12,
			34.56,
			[]byte{'b', 'y', 't', 'e', 's'},
			true,
			nil,
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(buf.Bytes()).To(Equal([]byte("~6\n" +
			"6\nstring\n" +
			"2\n12\n" +
			"5\n34.56\n" +
			"5\nbytes\n" +
			"1\n1\n" +
			"0\n" +
			"\n")))
	})

	It("should append time", func() {
		tm := time.Date(2019, 1, 1, 9, 45, 10, 222125, time.UTC)
		err := wr.WriteArgs([]interface{}{tm})
		Expect(err).NotTo(HaveOccurred())

		Expect(buf.Len()).To(Equal(37))
	})

	It("should append marshalable args", func() {
		err := wr.WriteArgs([]interface{}{&MyType{}})
		Expect(err).NotTo(HaveOccurred())

		Expect(buf.Len()).To(Equal(11))
	})

	It("should append net.IP", func() {
		ip := net.ParseIP("192.168.1.1")
		err := wr.WriteArgs([]interface{}{ip})
		Expect(err).NotTo(HaveOccurred())
		Expect(buf.String()).To(Equal(fmt.Sprintf("~1\n16\n%s\n", bytes.NewBuffer(ip))))
	})
})

type discard struct{}

func (discard) Write(b []byte) (int, error) {
	return len(b), nil
}

func (discard) WriteString(s string) (int, error) {
	return len(s), nil
}

func (discard) WriteByte(c byte) error {
	return nil
}

func BenchmarkWriteBuffer_Append(b *testing.B) {
	buf := proto.NewWriter(discard{})
	args := []interface{}{"hello", "world", "foo", "bar"}

	for i := 0; i < b.N; i++ {
		err := buf.WriteArgs(args)
		if err != nil {
			b.Fatal(err)
		}
	}
}
