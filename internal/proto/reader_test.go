package proto_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/satvik007/skytable-go/internal/proto"
)

func BenchmarkReader_ParseReply_Status(b *testing.B) {
	benchmarkParseReply(b, "!1\n0\n", false)
}

func BenchmarkReader_ParseReply_Int(b *testing.B) {
	benchmarkParseReply(b, ":2\n10\n", false)
}

func BenchmarkReader_ParseReply_Float(b *testing.B) {
	benchmarkParseReply(b, "%7\n123.456\n", false)
}

func BenchmarkReader_ParseReply_Error(b *testing.B) {
	benchmarkParseReply(b, "!13\nError message\n", true)
}

func BenchmarkReader_ParseReply_Nil(b *testing.B) {
	benchmarkParseReply(b, "!1\n1\n", true)
}

func BenchmarkReader_ParseReply_BinaryString(b *testing.B) {
	benchmarkParseReply(b, "?21\nSYNTAX invalid syntax\n", false)
}

func BenchmarkReader_ParseReply_String(b *testing.B) {
	benchmarkParseReply(b, "+5\nhello\n", false)
}

func BenchmarkReader_ParseReply_Array(b *testing.B) {
	benchmarkParseReply(b, "&2\n+5\nhello\n+5\nworld\n", false)
}

func TestReader_ReadLine(t *testing.T) {
	original := bytes.Repeat([]byte("a"), 8192)
	original[len(original)-1] = '\n'
	r := proto.NewReader(bytes.NewReader(original))
	read, err := r.ReadLine()
	if err != nil && err != io.EOF {
		t.Errorf("Should be able to read the full buffer: %v", err)
	}

	if bytes.Compare(read, original[:len(original)-1]) != 0 {
		t.Errorf("Values must be equal: %d expected %d", len(read), len(original[:len(original)-1]))
	}
}

func benchmarkParseReply(b *testing.B, reply string, wanterr bool) {
	buf := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		buf.WriteString(reply)
	}
	p := proto.NewReader(buf)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := p.ReadReply()
		if !wanterr && err != nil {
			b.Fatal(err)
		}
	}
}
