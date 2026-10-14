// Lever 5 pins for the posting request's notification step: with no active
// unread subscriber the unread bump issues no statement and allocates
// nothing, and an empty (already computed) mention list skips the webhook
// candidate lookup entirely.
package web

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basecamp/once-campfire-go/internal/database"
	"github.com/basecamp/once-campfire-go/internal/rails"
)

// notifyFixture builds a server over a counted database with the write lane
// on and fast reads off, so every read the notification step issues is a
// counted database/sql statement, plus one message in the owner's room.
func notifyFixture(t *testing.T) (*Server, *database.DB, database.Message, database.Room, func() int64, func(), func() []string) {
	t.Helper()
	t.Setenv("CAMPFIRE_WRITE_QUEUE", "on")
	t.Setenv("CAMPFIRE_FASTDB", "off")
	t.Setenv("CAMPFIRE_FASTDB_WRITE", "off")
	t.Setenv("CAMPFIRE_CHECKPOINT_MS", "3600000")
	t.Setenv("CAMPFIRE_FROZEN_TIME", "2026-01-02T03:04:05Z")
	root := t.TempDir()
	dbPath := filepath.Join(root, "test.sqlite3")
	db, count, reset, dump, err := database.OpenCounting(dbPath, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	user, err := db.Setup(ctx, "Owner", "owner@test", "digest")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateUser(ctx, "Alice", "alice@test", "digest", "", 0, nil); err != nil {
		t.Fatal(err)
	}
	rooms, err := db.Rooms(ctx, user.ID)
	if err != nil || len(rooms) == 0 {
		t.Fatalf("rooms: %v %v", rooms, err)
	}
	message, err := db.CreateMessage(ctx, user.ID, rooms[0].ID, "hello", "<p>hello</p>", "hello")
	if err != nil {
		t.Fatal(err)
	}
	secrets, err := rails.NewSecrets("notify-pins")
	if err != nil {
		t.Fatal(err)
	}
	app, err := New(db, secrets, false, dbPath, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Close)
	if app.Push.VAPID != nil {
		t.Fatal("test expects Web Push unconfigured (no VAPID env)")
	}
	return app, db, message, rooms[0], count, reset, dump
}

// TestMessageCreatedZeroSubscribersStatements pins the zero-subscriber post's
// notification step at zero statements: the unread bump reads no membership
// row, and an empty mention list runs no webhook candidate query. The posting
// request's own budget drops by exactly the one member read this replaces
// (TestCreateMessageStatementCount).
func TestMessageCreatedZeroSubscribersStatements(t *testing.T) {
	app, _, message, room, count, reset, dump := notifyFixture(t)
	ctx := context.Background()
	reset()
	app.messageCreated(ctx, message, room, nil)
	app.enqueueWebhooks(ctx, message, room, []int64{})
	if got := count(); got != 0 {
		t.Fatalf("zero-subscriber notification statements = %d, want 0\n%s", got, strings.Join(dump(), "\n"))
	}
}

// TestMessageCreatedZeroSubscribersAllocs pins the same shortcut's allocation
// budget: the bump allocates no payload map, no stream name and no frame when
// the room has nobody subscribed.
func TestMessageCreatedZeroSubscribersAllocs(t *testing.T) {
	app, _, message, room, _, _, _ := notifyFixture(t)
	ctx := context.Background()
	allocs := testing.AllocsPerRun(1000, func() {
		app.messageCreated(ctx, message, room, nil)
	})
	if allocs != 0 {
		t.Fatalf("zero-subscriber messageCreated = %.2f allocs/op, want 0", allocs)
	}
	webhookAllocs := testing.AllocsPerRun(1000, func() {
		app.enqueueWebhooks(ctx, message, room, []int64{})
	})
	if webhookAllocs != 0 {
		t.Fatalf("empty-mention enqueueWebhooks = %.2f allocs/op, want 0", webhookAllocs)
	}
}

// TestEnqueueWebhooksEmptyMentionsSkipsCandidates pins the mention-list
// shortcut against the deriving caller: a nil list still derives and queries
// (the bot path), while the create path's already-computed empty list does no
// work at all.
func TestEnqueueWebhooksEmptyMentionsSkipsCandidates(t *testing.T) {
	app, _, message, room, count, reset, _ := notifyFixture(t)
	ctx := context.Background()
	if room.Type == "Rooms::Direct" {
		t.Skip("the empty-mention shortcut applies to non-direct rooms")
	}
	reset()
	app.enqueueWebhooks(ctx, message, room, []int64{})
	if got := count(); got != 0 {
		t.Fatalf("empty-mention enqueueWebhooks statements = %d, want 0", got)
	}
	// A nil list (the bot and webhook callers) still derives the mentions from
	// the body; with none present that is still zero candidate queries.
	reset()
	app.enqueueWebhooks(ctx, message, room, nil)
	if got := count(); got != 0 {
		t.Fatalf("nil-mention enqueueWebhooks statements = %d, want 0", got)
	}
}
