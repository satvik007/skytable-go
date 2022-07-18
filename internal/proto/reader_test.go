package proto_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/satvik007/skytable-go/internal/proto"
)

func BenchmarkReader_ParseReply_Status(b *testing.B) {
	benchmarkParseReply(b, "+OK\n", false)
}

func BenchmarkReader_ParseReply_Int(b *testing.B) {
	benchmarkParseReply(b, ":1\n", false)
}

func BenchmarkReader_ParseReply_Float(b *testing.B) {
	benchmarkParseReply(b, ",123.456\n", false)
}

func BenchmarkReader_ParseReply_Bool(b *testing.B) {
	benchmarkParseReply(b, "#t\n", false)
}

func BenchmarkReader_ParseReply_BigInt(b *testing.B) {
	benchmarkParseReply(b, "(3492890328409238509324850943850943825024385\n", false)
}

func BenchmarkReader_ParseReply_Error(b *testing.B) {
	benchmarkParseReply(b, "-Error message\n", true)
}

func BenchmarkReader_ParseReply_Nil(b *testing.B) {
	benchmarkParseReply(b, "_\n", true)
}

func BenchmarkReader_ParseReply_BlobError(b *testing.B) {
	benchmarkParseReply(b, "!21\nSYNTAX invalid syntax", true)
}

func BenchmarkReader_ParseReply_String(b *testing.B) {
	benchmarkParseReply(b, "$5\nhello\n", false)
}

func BenchmarkReader_ParseReply_Verb(b *testing.B) {
	benchmarkParseReply(b, "$9\ntxt:hello\n", false)
}

func BenchmarkReader_ParseReply_Slice(b *testing.B) {
	benchmarkParseReply(b, "*2\n$5\nhello\n$5\nworld\n", false)
}

func BenchmarkReader_ParseReply_Set(b *testing.B) {
	benchmarkParseReply(b, "~2\n$5\nhello\n$5\nworld\n", false)
}

func BenchmarkReader_ParseReply_Push(b *testing.B) {
	benchmarkParseReply(b, ">2\n$5\nhello\n$5\nworld\n", false)
}

func BenchmarkReader_ParseReply_Attr(b *testing.B) {
	benchmarkParseReply(b, "%1\n+key\n+value\n+hello\n", false)
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
