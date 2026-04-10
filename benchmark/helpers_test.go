package benchmark

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/germtb/protodb/internal/testpb"
)

func makeItem(i int) *testpb.TestItem {
	return &testpb.TestItem{Name: fmt.Sprintf("item-%d", i), Value: int32(i)}
}

func mustMarshal(b *testing.B, msg proto.Message) []byte {
	b.Helper()
	data, err := proto.Marshal(msg)
	if err != nil {
		b.Fatal(err)
	}
	return data
}
