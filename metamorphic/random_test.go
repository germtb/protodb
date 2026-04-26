package metamorphic

import (
	"bytes"
	"encoding/binary"
	mathrand "math/rand/v2"
	"sort"
	"testing"

	"github.com/germtb/protodb"
)

type opMix struct {
	put, get, del, scan, flush, compact int
}

func (m opMix) total() int {
	return m.put + m.get + m.del + m.scan + m.flush + m.compact
}

type randomConfig struct {
	name     string
	seed     uint64
	ops      int
	keySpace uint64
	minValue int
	maxValue int
	mix      opMix
}

func keyOf(k uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], k)
	return b[:]
}

func runRandom(t *testing.T, cfg randomConfig) {
	t.Helper()
	engine, err := protodb.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer engine.Close()

	rng := mathrand.New(mathrand.NewPCG(cfg.seed, cfg.seed^0xdeadbeefcafebabe))
	ref := make(map[uint64][]byte)

	pickOp := func() byte {
		r := rng.IntN(cfg.mix.total())
		cum := cfg.mix.put
		if r < cum {
			return 'p'
		}
		cum += cfg.mix.get
		if r < cum {
			return 'g'
		}
		cum += cfg.mix.del
		if r < cum {
			return 'd'
		}
		cum += cfg.mix.scan
		if r < cum {
			return 's'
		}
		cum += cfg.mix.flush
		if r < cum {
			return 'f'
		}
		return 'c'
	}

	for i := 0; i < cfg.ops; i++ {
		switch pickOp() {
		case 'p':
			k := rng.Uint64N(cfg.keySpace)
			sz := cfg.minValue + rng.IntN(cfg.maxValue-cfg.minValue+1)
			v := make([]byte, sz)
			for j := range v {
				v[j] = byte(rng.UintN(256))
			}
			if err := engine.Put(keyOf(k), v); err != nil {
				t.Fatalf("op %d Put(%d): %v", i, k, err)
			}
			ref[k] = v

		case 'g':
			k := rng.Uint64N(cfg.keySpace)
			got, err := engine.Get(keyOf(k))
			if err != nil {
				t.Fatalf("op %d Get(%d): %v", i, k, err)
			}
			want, exists := ref[k]
			if !exists || want == nil {
				if got != nil {
					t.Fatalf("op %d Get(%d): got %d bytes, want nil", i, k, len(got))
				}
			} else if !bytes.Equal(got, want) {
				t.Fatalf("op %d Get(%d): value mismatch (got %d bytes, want %d)", i, k, len(got), len(want))
			}

		case 'd':
			k := rng.Uint64N(cfg.keySpace)
			if err := engine.Delete(keyOf(k)); err != nil {
				t.Fatalf("op %d Delete(%d): %v", i, k, err)
			}
			ref[k] = nil

		case 's':
			lo := rng.Uint64N(cfg.keySpace)
			span := uint64(1 + rng.IntN(int(cfg.keySpace/4)+1))
			hi := lo + span
			iter := engine.Scan(keyOf(lo), keyOf(hi))
			var gotKeys [][]byte
			gotValues := make(map[string][]byte)
			for iter.Next() {
				cur := iter.Current()
				k := append([]byte(nil), cur.Key...)
				v := append([]byte(nil), cur.Value...)
				gotKeys = append(gotKeys, k)
				gotValues[string(k)] = v
			}

			var wantKeys []uint64
			for rk, val := range ref {
				if rk >= lo && rk < hi && val != nil {
					wantKeys = append(wantKeys, rk)
				}
			}
			sort.Slice(wantKeys, func(i, j int) bool { return wantKeys[i] < wantKeys[j] })

			if len(gotKeys) != len(wantKeys) {
				t.Fatalf("op %d Scan(%d, %d): got %d entries, want %d", i, lo, hi, len(gotKeys), len(wantKeys))
			}
			for j, wk := range wantKeys {
				if !bytes.Equal(gotKeys[j], keyOf(wk)) {
					t.Fatalf("op %d Scan: pos %d key mismatch", i, j)
				}
				if !bytes.Equal(gotValues[string(keyOf(wk))], ref[wk]) {
					t.Fatalf("op %d Scan: value mismatch at key %d", i, wk)
				}
			}

		case 'f':
			if err := engine.Flush(); err != nil {
				t.Fatalf("op %d Flush: %v", i, err)
			}

		case 'c':
			if err := engine.Compact(); err != nil {
				t.Fatalf("op %d Compact: %v", i, err)
			}
		}
	}

	for k := uint64(0); k < cfg.keySpace; k++ {
		got, err := engine.Get(keyOf(k))
		if err != nil {
			t.Fatalf("final Get(%d): %v", k, err)
		}
		want, exists := ref[k]
		if !exists || want == nil {
			if got != nil {
				t.Fatalf("final Get(%d): got %d bytes, want nil", k, len(got))
			}
		} else if !bytes.Equal(got, want) {
			t.Fatalf("final Get(%d): value mismatch", k)
		}
	}
}

var randomConfigs = []randomConfig{
	{name: "balanced", seed: 1, ops: 10000, keySpace: 500, minValue: 100, maxValue: 2000,
		mix: opMix{put: 4, get: 2, del: 1, scan: 1, flush: 1, compact: 1}},
	{name: "denseSmallKeys", seed: 2, ops: 5000, keySpace: 64, minValue: 1, maxValue: 64,
		mix: opMix{put: 4, get: 2, del: 1, scan: 1, flush: 1, compact: 1}},
	{name: "writeHeavy", seed: 3, ops: 8000, keySpace: 1000, minValue: 50, maxValue: 500,
		mix: opMix{put: 8, get: 1, del: 1, scan: 0, flush: 1, compact: 1}},
	{name: "readHeavy", seed: 4, ops: 8000, keySpace: 1000, minValue: 50, maxValue: 500,
		mix: opMix{put: 1, get: 8, del: 0, scan: 1, flush: 0, compact: 0}},
	{name: "deleteHeavy", seed: 5, ops: 5000, keySpace: 200, minValue: 1, maxValue: 100,
		mix: opMix{put: 2, get: 1, del: 4, scan: 1, flush: 1, compact: 1}},
	{name: "largeValues", seed: 6, ops: 2000, keySpace: 50, minValue: 4096, maxValue: 32768,
		mix: opMix{put: 4, get: 2, del: 1, scan: 1, flush: 1, compact: 1}},
	{name: "frequentFlush", seed: 7, ops: 5000, keySpace: 200, minValue: 100, maxValue: 1000,
		mix: opMix{put: 4, get: 2, del: 1, scan: 1, flush: 4, compact: 0}},
}

func TestRandomWorkloads(t *testing.T) {
	for _, cfg := range randomConfigs {
		cfg := cfg
		t.Run(cfg.name, func(t *testing.T) {
			t.Parallel()
			runRandom(t, cfg)
		})
	}
}
