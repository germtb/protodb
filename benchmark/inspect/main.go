package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/germtb/protodb"
)

func main() {
	doCompact := flag.Bool("compact", false, "force compaction before printing stats")
	flag.Parse()
	if flag.NArg() < 1 {
		fmt.Println("usage: inspect [-compact] <dir>")
		os.Exit(2)
	}
	dir := flag.Arg(0)
	eng, err := protodb.Open(dir)
	if err != nil {
		fmt.Println("open:", err)
		os.Exit(1)
	}
	defer eng.Close()
	if *doCompact {
		if err := eng.Compact(); err != nil {
			fmt.Println("compact:", err)
			os.Exit(1)
		}
	}
	st := eng.Stats()
	fmt.Printf("L0=%d (%dB) L1=%d (%dB)\n", st.L0SSTs, st.L0Bytes, st.L1SSTs, st.L1Bytes)
}
