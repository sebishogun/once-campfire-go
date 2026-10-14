// Lever 5 tests: the unread-bump publication that targets the hub's ACTIVE
// unread subscriptions instead of the room's membership. Delivery is pinned
// across the two dimensions the old fan-out scaled with — room members (which
// must no longer matter) and active subscribers — together with the
// zero-subscriber shortcut's statement/allocation budget, the authorization
// rechecks (member, revoked session) and the shortcut's concurrency point.
package cable

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basecamp/once-campfire-go/internal/database"
	"github.com/basecamp/once-campfire-go/internal/rails"
)

// unreadIdentifier is the subscription identifier an UnreadRoomsChannel
// client uses (the rooms-list Stimulus controller subscribes with the channel
// name alone, so every client's identifier is this one string).
const unreadIdentifier = `{"channel":"UnreadRoomsChannel"}`

// unreadSubscription is the stored subscription for a client's own unread
// stream, exactly what Hub.subscription derives for UnreadRoomsChannel.
func unreadSubscription(user database.User) subscription {
	return subscription{Channel: "UnreadRoomsChannel", Stream: fmt.Sprintf("user_%d_unreads", user.ID)}
}

// unreadSubscriber starts a session for user and registers a simulated client
// subscribed to that user's unread stream, returning the client and its token.
func unreadSubscriber(t testing.TB, hub *Hub, db *database.DB, user database.User) (*client, string) {
	t.Helper()
	token, err := db.StartSession(context.Background(), user.ID, "unreads", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	c := simulateClient(t, user, token, unreadIdentifier, unreadSubscription(user))
	hub.register(c)
	return c, token
}

// bumpPayload is the frame message the bump must carry: the room id.
func bumpPayload(room int64) string { return `{"roomId":` + strconv.FormatInt(room, 10) + `}` }

// expectBump asserts c received exactly one bump frame for room.
func expectBump(t testing.TB, ctx context.Context, c *client, room int64) {
	t.Helper()
	f := testRecv(ctx, c)
	if f == nil {
		t.Fatal("no unread bump delivered")
	}
	var frame struct {
		Identifier string          `json:"identifier"`
		Message    json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(f.Data(), &frame); err != nil {
		t.Fatalf("undecodable bump frame %q: %v", f.Data(), err)
	}
	if frame.Identifier != unreadIdentifier {
		t.Fatalf("bump identifier = %q, want %q", frame.Identifier, unreadIdentifier)
	}
	if string(frame.Message) != bumpPayload(room) {
		t.Fatalf("bump payload = %q, want %q", frame.Message, bumpPayload(room))
	}
}

// TestUnreadBumpDeliveryMatrix pins the bump across subscriber counts
// (0/1/100) and room-member counts (2/100/1,000): every active subscriber that
// is a member receives exactly one frame, and the member count changes
// nothing about that — the bump no longer reads the membership at all.
func TestUnreadBumpDeliveryMatrix(t *testing.T) {
	for _, members := range []int{2, 100, 1000} {
		for _, subs := range []int{0, 1, 100} {
			if subs > members {
				continue
			}
			t.Run(fmt.Sprintf("members=%d/subs=%d", members, subs), func(t *testing.T) {
				hub, db, owner, roomID, _ := hubFixture(t)
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				// CreateUser grants the new user a membership in every open
				// room (the fixture room is the open one), so the member
				// count is the number of users without touching memberships.
				users := []database.User{owner}
				for i := 1; i < members; i++ {
					u, err := db.CreateUser(ctx, fmt.Sprintf("Member%d", i), fmt.Sprintf("m%d@test", i), "digest", "", 0, nil)
					if err != nil {
						t.Fatal(err)
					}
					users = append(users, u)
				}
				clients := make([]*client, 0, subs)
				for i := 0; i < subs; i++ {
					c, _ := unreadSubscriber(t, hub, db, users[i])
					clients = append(clients, c)
				}
				if err := hub.PublishUnreadBump(ctx, roomID); err != nil {
					t.Fatal(err)
				}
				for _, c := range clients {
					expectBump(t, ctx, c, roomID)
					expectNone(t, c) // exactly one frame per subscriber
				}
			})
		}
	}
}

// TestUnreadBumpAuthorizationRecheck pins both halves of the publish-time
// recheck: a live subscriber that is not a member of the posted room is
// skipped and stays connected, while a subscriber whose session was revoked
// is excluded and stopped (the generic publish path's rule for an
// unauthorized recipient).
func TestUnreadBumpAuthorizationRecheck(t *testing.T) {
	hub, db, owner, _, _ := hubFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	revoked, err := db.CreateUser(ctx, "Revoked", "revoked@test", "digest", "", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	// A closed room with the owner and the soon-to-be-revoked user as members;
	// the outsider is a member of the open room only, never of this one.
	closed, err := db.CreateRoom(ctx, owner.ID, "Rooms::Closed", "Quiet", []int64{owner.ID, revoked.ID})
	if err != nil {
		t.Fatal(err)
	}
	outsider, err := db.CreateUser(ctx, "Outsider", "outsider@test", "digest", "", 0, nil)
	if err != nil {
		t.Fatal(err)
	}

	memberC, _ := unreadSubscriber(t, hub, db, owner)
	nonMemberC, _ := unreadSubscriber(t, hub, db, outsider)
	revokedC, revokedToken := unreadSubscriber(t, hub, db, revoked)
	if err := db.DeleteSession(ctx, revokedToken, revoked.ID); err != nil {
		t.Fatal(err)
	}

	if err := hub.PublishUnreadBump(ctx, closed.ID); err != nil {
		t.Fatal(err)
	}
	expectBump(t, ctx, memberC, closed.ID)
	expectNone(t, nonMemberC)
	if nonMemberC.dying.Load() {
		t.Fatal("non-member subscriber was disconnected by the bump")
	}
	expectNone(t, revokedC)
	if !revokedC.dying.Load() {
		t.Fatal("revoked subscriber was not disconnected by the bump")
	}
}

// TestUnreadBumpZeroSubscriberShortcut pins the shortcut: with no active
// unread subscription the bump takes the hub lock, sees an empty index and
// returns — zero statements and zero allocations per call.
func TestUnreadBumpZeroSubscriberShortcut(t *testing.T) {
	hub, _, _, roomID, _ := hubFixture(t)
	ctx := context.Background()
	if err := hub.PublishUnreadBump(ctx, roomID); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if err := hub.PublishUnreadBump(ctx, roomID); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("zero-subscriber unread bump = %.2f allocs/op, want 0", allocs)
	}
}

// TestUnreadBumpStatementBudget pins the statement count of one bump: zero
// for a room nobody is subscribed to, exactly one — the bulk authorization —
// for a room with subscribers, whatever the room's member count.
func TestUnreadBumpStatementBudget(t *testing.T) {
	root := t.TempDir()
	db, count, reset, dump, err := database.OpenCounting(root+"/counted.sqlite3", 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	owner, err := db.Setup(ctx, "Owner", "owner@test", "digest")
	if err != nil {
		t.Fatal(err)
	}
	rooms, err := db.Rooms(ctx, owner.ID)
	if err != nil || len(rooms) == 0 {
		t.Fatalf("rooms: %v %v", rooms, err)
	}
	secrets, err := rails.NewSecrets("unread-bump-counted")
	if err != nil {
		t.Fatal(err)
	}
	hub := New(db, secrets)
	t.Cleanup(hub.Close)

	reset()
	if err := hub.PublishUnreadBump(ctx, rooms[0].ID); err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 0 {
		t.Fatalf("zero-subscriber bump statements = %d, want 0\n%s", got, strings.Join(dump(), "\n"))
	}

	// A 100-member room with one subscriber still costs exactly one query.
	for i := 1; i < 100; i++ {
		if _, err := db.CreateUser(ctx, fmt.Sprintf("Member%d", i), fmt.Sprintf("m%d@test", i), "digest", "", 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	unreadSubscriber(t, hub, db, owner)
	reset()
	if err := hub.PublishUnreadBump(ctx, rooms[0].ID); err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 1 {
		t.Fatalf("one-subscriber bump statements = %d, want 1\n%s", got, strings.Join(dump(), "\n"))
	}
}

// TestUnreadBumpConcurrencyPoint pins the shortcut's linearization: the hub
// lock snapshot is the publication's concurrency point, so a subscription
// registered before the bump is delivered to and one removed before the bump
// is not.
func TestUnreadBumpConcurrencyPoint(t *testing.T) {
	hub, db, owner, roomID, _ := hubFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, _ := unreadSubscriber(t, hub, db, owner)

	if err := hub.PublishUnreadBump(ctx, roomID); err != nil {
		t.Fatal(err)
	}
	expectBump(t, ctx, c, roomID)

	hub.mu.Lock()
	delete(c.subscriptions, unreadIdentifier)
	hub.indexRemoveLocked(c, unreadIdentifier, unreadSubscription(owner))
	hub.mu.Unlock()
	if err := hub.PublishUnreadBump(ctx, roomID); err != nil {
		t.Fatal(err)
	}
	expectNone(t, c)

	// Re-subscribing restores delivery (the index is the only gate).
	hub.mu.Lock()
	c.subscriptions[unreadIdentifier] = unreadSubscription(owner)
	hub.indexAddLocked(c, unreadIdentifier, unreadSubscription(owner))
	hub.mu.Unlock()
	if err := hub.PublishUnreadBump(ctx, roomID); err != nil {
		t.Fatal(err)
	}
	expectBump(t, ctx, c, roomID)
}

// TestUnreadBumpConcurrentChurn exercises the bump against concurrent
// subscribe/unsubscribe churn under -race: every frame any client observes
// must be a bump frame, and once the churn has stopped a final bump must
// reach every client that is still subscribed (the index and the client set
// stay consistent).
func TestUnreadBumpConcurrentChurn(t *testing.T) {
	hub, db, owner, roomID, _ := hubFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const clientsN, bumpers, perBumper = 32, 3, 40
	clients := make([]*client, 0, clientsN)
	for i := 0; i < clientsN; i++ {
		c, _ := unreadSubscriber(t, hub, db, owner)
		clients = append(clients, c)
	}
	want := bumpPayload(roomID)
	var drainWG sync.WaitGroup
	stormOver := make(chan struct{})
	for _, c := range clients {
		drainWG.Add(1)
		go func(c *client) {
			defer drainWG.Done()
			for {
				f := testRecvNow(c)
				if f == nil {
					select {
					case <-stormOver:
						return
					case <-time.After(time.Millisecond):
						continue
					}
				}
				var frame struct {
					Identifier string          `json:"identifier"`
					Message    json.RawMessage `json:"message"`
				}
				if err := json.Unmarshal(f.Data(), &frame); err != nil || frame.Identifier != unreadIdentifier || string(frame.Message) != want {
					t.Errorf("client %p received non-bump frame %q", c, f.Data())
				}
			}
		}(c)
	}
	var wg sync.WaitGroup
	for i := 0; i < bumpers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perBumper; j++ {
				if err := hub.PublishUnreadBump(ctx, roomID); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		sub := unreadSubscription(owner)
		for j := 0; j < perBumper; j++ {
			for _, c := range clients {
				hub.mu.Lock()
				delete(c.subscriptions, unreadIdentifier)
				hub.indexRemoveLocked(c, unreadIdentifier, sub)
				hub.mu.Unlock()
			}
			for _, c := range clients {
				hub.mu.Lock()
				c.subscriptions[unreadIdentifier] = sub
				hub.indexAddLocked(c, unreadIdentifier, sub)
				hub.mu.Unlock()
			}
		}
	}()
	wg.Wait()
	close(stormOver)
	drainWG.Wait()
	// A drainer's final empty check can lose the race with a bump that landed
	// just before the storm ended, so clear whatever storm frames are left.
	for _, c := range clients {
		c.q.clear()
	}
	// Every client is subscribed again: the final bump reaches all of them.
	if err := hub.PublishUnreadBump(ctx, roomID); err != nil {
		t.Fatal(err)
	}
	for _, c := range clients {
		expectBump(t, ctx, c, roomID)
		expectNone(t, c)
	}
}
