package metamorphic

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/germtb/protodb"
)

// workload is a short deterministic op sequence exercising the main write
// paths: WAL append, memtable flush, compaction (enough flushes to exceed
// SoftCompactionThreshold=4), and reads that force SST lookup.
func workload(engine *protodb.Engine) error {
	for batch := 0; batch < 6; batch++ {
		for i := 0; i < 20; i++ {
			k := []byte(fmt.Sprintf("k%03d-%03d", batch, i))
			if err := engine.Put(k, []byte("v")); err != nil {
				return err
			}
		}
		if err := engine.Flush(); err != nil {
			return err
		}
	}
	if err := engine.Compact(); err != nil {
		return err
	}
	for batch := 0; batch < 6; batch++ {
		k := []byte(fmt.Sprintf("k%03d-%03d", batch, 0))
		if _, err := engine.Get(k); err != nil {
			return err
		}
	}
	for i := 0; i < 10; i++ {
		if err := engine.Delete([]byte(fmt.Sprintf("k000-%03d", i))); err != nil {
			return err
		}
	}
	return engine.Flush()
}

// TestErrorInjectionSweep mirrors Pebble's own error_test.go TestErrors: for
// each index i, wrap the FS so the (i+1)-th op faults, run a fixed
// workload, then reopen with a clean FS and assert no corruption.
//
// The sweep terminates when a run completes without any injected fault —
// at that point i has passed every op site in the workload.
//
// Invariants enforced:
//  1. Any error that escapes Open/workload must be ErrInjected (nothing
//     else should fail). Swallowed-error bugs surface as "injected=false"
//     on a cut-point well below the clean op count — treat those as a test
//     failure.
//  2. After an injected failure, reopening the DB with a plain FS must
//     succeed and every Get must return either the written value or nil —
//     never a corrupt byte.
func TestErrorInjectionSweep(t *testing.T) {
	run := func(mem vfs.FS, inj errorfs.Injector) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		fs := Wrap(errorfs.Wrap(mem, inj))
		engine, err := protodb.Open("/db", protodb.WithFS(fs))
		if err != nil {
			return err
		}
		defer engine.Close()
		return workload(engine)
	}

	verifyReopen := func(mem vfs.FS) error {
		engine, err := protodb.Open("/db", protodb.WithFS(Wrap(mem)))
		if err != nil {
			return fmt.Errorf("reopen: %w", err)
		}
		defer engine.Close()
		for batch := 0; batch < 6; batch++ {
			for i := 0; i < 20; i++ {
				k := []byte(fmt.Sprintf("k%03d-%03d", batch, i))
				got, err := engine.Get(k)
				if err != nil {
					return fmt.Errorf("get %s: %w", k, err)
				}
				if got != nil && string(got) != "v" {
					return fmt.Errorf("get %s: corrupt value %q", k, got)
				}
			}
		}
		return nil
	}

	// Measure the clean-run op count so we can detect swallowed errors: a
	// cut-point i < cleanOps that fails to inject means protodb dropped the
	// error from some FS call on the happy path.
	var cleanOps int32
	{
		mem := vfs.NewMem()
		counting := errorfs.InjectorFunc(func(errorfs.Op, string) error {
			cleanOps++
			return nil
		})
		if err := run(mem, counting); err != nil {
			t.Fatalf("clean run failed: %v", err)
		}
	}
	t.Logf("clean workload fires %d FS ops", cleanOps)

	for i := int32(0); i < cleanOps; i++ {
		mem := vfs.NewMem()
		runErr := run(mem, errorfs.OnIndex(i))
		if runErr == nil {
			t.Fatalf("i=%d (< cleanOps=%d): injection fired but no error surfaced — swallowed error in protodb", i, cleanOps)
		}
		if !errors.Is(runErr, errorfs.ErrInjected) {
			t.Fatalf("i=%d: non-injected error: %v", i, runErr)
		}
		if err := verifyReopen(mem); err != nil {
			t.Fatalf("i=%d: %v", i, err)
		}
	}
}
