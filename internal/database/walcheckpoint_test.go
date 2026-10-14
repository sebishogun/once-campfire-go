package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// walFileSize reports the database's -wal file size (0 when the file is
// absent, as it is after the last connection closes).
func walFileSize(path string) int64 {
	if info, err := os.Stat(path + "-wal"); err == nil {
		return info.Size()
	}
	return 0
}

// seedPoster opens a database at path and returns it with one user id and one
// room id to post to.
func seedPoster(t *testing.T, path string, readers int) (*DB, int64, int64) {
	t.Helper()
	d, err := Open(path, readers)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	user, err := d.Setup(ctx, "Owner", "owner@test", "digest")
	if err != nil {
		d.Close()
		t.Fatal(err)
	}
	rooms, err := d.Rooms(ctx, user.ID)
	if err != nil || len(rooms) == 0 {
		d.Close()
		t.Fatalf("rooms: %v %v", rooms, err)
	}
	return d, user.ID, rooms[0].ID
}

// TestWALStaysBoundedUnderSustainedWrites is the Lever 4 contract: with the
// growth ceiling on (the default), a sustained post workload must keep the WAL
// file bounded, and the run must still end with the same durable state — the
// writer drains, the final checkpoint runs, the WAL is removed, and every
// committed message survives a reopen. Before the ceiling, the same workload
// reached thousands of megabytes (readers starve SQLite's own WAL restart).
func TestWALStaysBoundedUnderSustainedWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.sqlite3")
	d, user, room := seedPoster(t, path, 4)
	ctx := context.Background()
	limit := d.writer.limit
	if limit <= 0 {
		t.Fatalf("growth ceiling = %d, want the default on", limit)
	}
	frame := pragmaInt(t, d.Write, "PRAGMA page_size") + 24
	const posts = 4000
	var maxWAL int64
	for i := 0; i < posts; i++ {
		if _, err := d.CreateMessage(ctx, user, room, fmt.Sprintf("w-%d", i), "<p>bounded</p>", "bounded"); err != nil {
			t.Fatal(err)
		}
		if i%50 == 0 {
			if size := walFileSize(path); size > maxWAL {
				maxWAL = size
			}
		}
	}
	if size := walFileSize(path); size > maxWAL {
		maxWAL = size
	}
	if restarts := d.writer.restarts.Load(); restarts == 0 {
		t.Fatal("no WAL restarts under sustained writes; the ceiling never fired")
	}
	// The WAL may overshoot the ceiling by the frames written between checks
	// and by its high-water mark, but it must stay near the ceiling rather
	// than grow without bound.
	if bound := (limit + 4000) * frame; maxWAL > bound {
		t.Fatalf("WAL reached %d bytes, want under %d (ceiling %d frames)", maxWAL, bound, limit)
	}

	// Final drain: Close drains the writer, runs the last PASSIVE checkpoint
	// and removes the WAL with the last connection.
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if size := walFileSize(path); size != 0 {
		t.Fatalf("WAL remains after Close: %d bytes", size)
	}
	reopened, err := Open(path, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var count int
	if err := reopened.Read.QueryRow("SELECT count(*) FROM messages").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != posts {
		t.Fatalf("messages after reopen = %d, want %d", count, posts)
	}
}

// TestWALGrowthCeilingRollbackSwitch pins the rollback switch: with
// CAMPFIRE_CHECKPOINT_PAGES=0 the writer performs no restarts and the WAL is
// free to pass the default ceiling, which is exactly the pre-change policy.
func TestWALGrowthCeilingRollbackSwitch(t *testing.T) {
	t.Setenv("CAMPFIRE_CHECKPOINT_PAGES", "0")
	path := filepath.Join(t.TempDir(), "wal.sqlite3")
	d, user, room := seedPoster(t, path, 4)
	defer d.Close()
	ctx := context.Background()
	if d.writer.limit != 0 {
		t.Fatalf("growth ceiling = %d, want 0 with the switch off", d.writer.limit)
	}
	frame := pragmaInt(t, d.Write, "PRAGMA page_size") + 24
	for i := 0; i < 1500; i++ {
		if _, err := d.CreateMessage(ctx, user, room, fmt.Sprintf("u-%d", i), "<p>unbounded</p>", "unbounded"); err != nil {
			t.Fatal(err)
		}
	}
	if restarts := d.writer.restarts.Load(); restarts != 0 {
		t.Fatalf("restarts = %d with the ceiling off, want 0", restarts)
	}
	if size := walFileSize(path); size <= 10000*frame {
		t.Fatalf("WAL = %d bytes, want past the default ceiling %d with the limit off", size, 10000*frame)
	}
}

// TestCheckpointTelemetryCounters pins the telemetry: one PASSIVE checkpoint
// records its duration, the frames it moved, the WAL size and the backlog, and
// the counters move only when a checkpoint runs.
func TestCheckpointTelemetryCounters(t *testing.T) {
	t.Setenv("CAMPFIRE_CHECKPOINT_MS", "3600000") // no timed ticks: passive runs explicitly
	path := filepath.Join(t.TempDir(), "wal.sqlite3")
	d, user, room := seedPoster(t, path, 4)
	defer d.Close()
	ctx := context.Background()
	for i := 0; i < 40; i++ {
		if _, err := d.CreateMessage(ctx, user, room, fmt.Sprintf("t-%d", i), "<p>telemetry</p>", "telemetry"); err != nil {
			t.Fatal(err)
		}
	}
	before := d.checkpoints.snapshot()
	d.checkpoints.passive()
	after := d.checkpoints.snapshot()
	if after.Checkpoints != before.Checkpoints+1 {
		t.Fatalf("checkpoints = %d, want %d", after.Checkpoints, before.Checkpoints+1)
	}
	if after.MovedTotal <= before.MovedTotal {
		t.Fatalf("frames moved did not grow: %d then %d", before.MovedTotal, after.MovedTotal)
	}
	if after.Moved <= 0 {
		t.Fatalf("latest checkpoint moved %d frames, want > 0", after.Moved)
	}
	if after.LastUnixNano == 0 {
		t.Fatal("no checkpoint timestamp recorded")
	}
	if after.WALBytes == 0 {
		t.Fatal("no WAL size recorded")
	}
	if after.DurNanos <= 0 {
		t.Fatal("no checkpoint duration recorded")
	}
	// A second checkpoint with nothing new to move is the no-op the fixed
	// interval pays for; the counter must see it.
	d.checkpoints.passive()
	noop := d.checkpoints.snapshot()
	if noop.Noops <= after.Noops {
		t.Fatalf("no-op checkpoints = %d, want more than %d", noop.Noops, after.Noops)
	}
}

// TestCheckpointPolicyConfigSemantics pins the knobs' shapes: 0 disables the
// timed fallback (the growth ceiling schedules), 0 on both restores the 1s
// fallback so a run still checkpoints, and invalid values keep the defaults.
func TestCheckpointPolicyConfigSemantics(t *testing.T) {
	t.Run("interval zero leaves the growth ceiling in charge", func(t *testing.T) {
		t.Setenv("CAMPFIRE_CHECKPOINT_MS", "0")
		d := testDB(t)
		if d.checkpoints.interval != 0 {
			t.Fatalf("interval = %v, want 0 (timed fallback off)", d.checkpoints.interval)
		}
		if d.writer.limit <= 0 {
			t.Fatalf("growth ceiling = %d, want the default on", d.writer.limit)
		}
	})
	t.Run("both zero restores the fallback", func(t *testing.T) {
		t.Setenv("CAMPFIRE_CHECKPOINT_MS", "0")
		t.Setenv("CAMPFIRE_CHECKPOINT_PAGES", "0")
		d := testDB(t)
		if d.checkpoints.interval != time.Second {
			t.Fatalf("interval = %v, want 1s when nothing else schedules", d.checkpoints.interval)
		}
	})
	t.Run("invalid values keep the defaults", func(t *testing.T) {
		t.Setenv("CAMPFIRE_CHECKPOINT_MS", "-5")
		t.Setenv("CAMPFIRE_CHECKPOINT_PAGES", "not-a-number")
		d := testDB(t)
		if d.checkpoints.interval != time.Second {
			t.Fatalf("interval = %v, want the 1s default", d.checkpoints.interval)
		}
		if d.writer.limit != 10000 {
			t.Fatalf("growth ceiling = %d, want the 10000-frame default", d.writer.limit)
		}
	})
}
