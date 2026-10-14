// Lever 5 tests: the unread-bump publication that intersects the hub's ACTIVE
// unread subscriptions with the room's members. Delivery is pinned across the
// two dimensions the old fan-out scaled with — room members (which must no
// longer cost per-member publishes) and active subscribers — together with the
// candidate strategy's scaling (unrelated subscribers cost nothing), the
// zero-subscriber shortcut's statement/allocation budget, the authorization
// rechecks and the snapshot boundary that makes the shortcut safe.
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

// unreadBudget is the budget the tests hand the bump: generous, so only the
// deadline behaviour under test can fail a call.
const unreadBudget = 5 * time.Second

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

// strayUnreadSubscriber registers a simulated client whose user is NOT a
// member of any test room, subscribed to its own unread stream. It is built
// directly rather than through the database: the point is the hub's candidate
// set, and a real user row plus session per unrelated subscriber would turn
// the 10,000 case into a database benchmark instead of a hub one. Its token is
// deliberately not a live session, so a publication that looked it up would
// find it unauthorized and stop the client — which is how the tests below
// prove the token never reaches the query.
func strayUnreadSubscriber(hub *Hub, id int64, cancel context.CancelFunc) *client {
	user := database.User{ID: id}
	c := &client{
		user:          user,
		token:         fmt.Sprintf("stray-token-%d", id),
		cancel:        cancel,
		q:             newOutQueueCap(2),
		subscriptions: map[string]subscription{unreadIdentifier: unreadSubscription(user)},
	}
	hub.register(c)
	return c
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

// expectSilent asserts a subscriber received nothing and is still connected.
func expectSilent(t testing.TB, c *client) {
	t.Helper()
	expectNone(t, c)
	if c.dying.Load() {
		t.Fatal("subscriber was disconnected by a publication it was not a candidate of")
	}
}

// TestUnreadBumpDeliveryMatrix pins the bump across subscriber counts
// (0/1/100) and room-member counts (2/100/1,000): every active subscriber that
// is a member receives exactly one frame, whatever the member count.
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
				if err := hub.PublishUnreadBump(roomID, unreadBudget); err != nil {
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

// TestUnreadBumpRoomRelevantCandidates pins the candidate strategy with a fixed
// two-member target room and 0/100/10,000 unrelated subscribers: the statement
// count, the allocation count and the lock-held candidate work are flat in the
// unrelated count, the unrelated subscribers are never candidates (they are
// not delivered to and not disconnected, which a token list containing them
// would do, since their sessions do not exist), and the room's own subscriber
// still costs one member read plus one bulk authorization.
func TestUnreadBumpRoomRelevantCandidates(t *testing.T) {
	var allocsByStrays []float64
	var straysByCount []int
	for _, strays := range []int{0, 100, 10000} {
		t.Run(fmt.Sprintf("strays=%d", strays), func(t *testing.T) {
			db, count, reset, _, err := database.OpenCounting(t.TempDir()+"/counted.sqlite3", 4)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			owner, err := db.Setup(ctx, "Owner", "owner@test", "digest")
			if err != nil {
				t.Fatal(err)
			}
			member, err := db.CreateUser(ctx, "Member", "member@test", "digest", "", 0, nil)
			if err != nil {
				t.Fatal(err)
			}
			// The target: a closed room with exactly two members.
			pair, err := db.CreateRoom(ctx, owner.ID, "Rooms::Closed", "Pair", []int64{owner.ID, member.ID})
			if err != nil {
				t.Fatal(err)
			}
			members, err := db.RoomMemberIDs(ctx, pair.ID)
			if err != nil || len(members) != 2 {
				t.Fatalf("target room members: %v %v", members, err)
			}
			secrets, err := rails.NewSecrets("unread-candidates")
			if err != nil {
				t.Fatal(err)
			}
			hub := New(db, secrets)
			t.Cleanup(hub.Close)
			_, cancelStrays := context.WithCancel(context.Background())
			defer cancelStrays()
			straysList := make([]*client, 0, strays)
			for i := 0; i < strays; i++ {
				straysList = append(straysList, strayUnreadSubscriber(hub, 1_000_000+int64(i), cancelStrays))
			}

			// 1. Nobody in the target room is subscribed. With no unread
			// subscriber anywhere the bump is the zero-cost shortcut; with
			// unrelated subscribers it is exactly the two-row member read,
			// flat in the unrelated count.
			wantStatements := int64(0)
			if strays > 0 {
				wantStatements = 1
			}
			reset()
			if err := hub.PublishUnreadBump(pair.ID, unreadBudget); err != nil {
				t.Fatal(err)
			}
			// Behaviour first: an unrelated subscriber is not a candidate, so
			// it is neither delivered to nor disconnected (its token is not a
			// live session, so a publication that looked it up would stop it).
			for _, c := range straysList {
				expectSilent(t, c)
			}
			if got := count(); got != wantStatements {
				t.Fatalf("bump statements = %d, want %d (member read only)", got, wantStatements)
			}
			if strays > 0 {
				allocsByStrays = append(allocsByStrays, testing.AllocsPerRun(200, func() {
					if err := hub.PublishUnreadBump(pair.ID, unreadBudget); err != nil {
						t.Fatal(err)
					}
				}))
				straysByCount = append(straysByCount, strays)
			}

			// 2. The room's member subscribes: one member read plus one bulk
			// authorization, still flat in the unrelated count, and only the
			// member is delivered to.
			memberC, _ := unreadSubscriber(t, hub, db, member)
			reset()
			if err := hub.PublishUnreadBump(pair.ID, unreadBudget); err != nil {
				t.Fatal(err)
			}
			expectBump(t, ctx, memberC, pair.ID)
			expectNone(t, memberC)
			for _, c := range straysList {
				expectSilent(t, c)
			}
			if got := count(); got != 2 {
				t.Fatalf("bump statements = %d, want 2 (member read + one authorization)", got)
			}
		})
	}
	// The allocation count of the candidate work must not move between 100 and
	// 10,000 unrelated subscribers: neither the member read, the token list nor
	// the snapshot touches them. A per-subscriber implementation would add
	// thousands here (one token each, plus the JSON array); the slack absorbs
	// the runtime's own measurement noise under -race, and the absolute bound
	// keeps the slack from hiding a real regression. TestUnreadBumpRoomRelevant
	// Candidates' delivery assertions are the exact half of this pin.
	if len(allocsByStrays) != 2 || allocsByStrays[1] > allocsByStrays[0]+4 || allocsByStrays[1] > 200 {
		t.Fatalf("candidate allocations grew with unrelated subscribers: %v (counts %v)", allocsByStrays, straysByCount)
	}
}

// TestUnreadBumpAuthorizationRecheck pins the publish-time recheck for a
// candidate: a subscriber whose session was revoked is excluded and stopped
// (the generic publish path's rule for an unauthorized recipient), while a
// live subscriber that is not a member of the posted room is not a candidate
// at all — it receives nothing and stays connected.
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

	if err := hub.PublishUnreadBump(closed.ID, unreadBudget); err != nil {
		t.Fatal(err)
	}
	expectBump(t, ctx, memberC, closed.ID)
	expectSilent(t, nonMemberC)
	expectNone(t, revokedC)
	if !revokedC.dying.Load() {
		t.Fatal("revoked subscriber was not disconnected by the bump")
	}
}

// TestUnreadBumpZeroSubscriberShortcut pins the shortcut: with no unread
// subscription anywhere the bump sees an empty index and returns — zero
// statements and zero allocations per call, no timer.
func TestUnreadBumpZeroSubscriberShortcut(t *testing.T) {
	hub, _, _, roomID, _ := hubFixture(t)
	if err := hub.PublishUnreadBump(roomID, unreadBudget); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if err := hub.PublishUnreadBump(roomID, unreadBudget); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("zero-subscriber unread bump = %.2f allocs/op, want 0", allocs)
	}
}

// TestUnreadBumpStatementBudget pins the statement count of one bump: zero for
// a server with no unread subscriber, and for a room whose member is the only
// subscriber exactly the member read plus ONE bulk authorization — never one
// statement per member.
func TestUnreadBumpStatementBudget(t *testing.T) {
	db, count, reset, dump, err := database.OpenCounting(t.TempDir()+"/counted.sqlite3", 4)
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
	if err := hub.PublishUnreadBump(rooms[0].ID, unreadBudget); err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 0 {
		t.Fatalf("zero-subscriber bump statements = %d, want 0\n%s", got, strings.Join(dump(), "\n"))
	}

	// A 100-member room with one subscriber still costs one member read and
	// one authorization, not one publish per member.
	for i := 1; i < 100; i++ {
		if _, err := db.CreateUser(ctx, fmt.Sprintf("Member%d", i), fmt.Sprintf("m%d@test", i), "digest", "", 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	unreadSubscriber(t, hub, db, owner)
	reset()
	if err := hub.PublishUnreadBump(rooms[0].ID, unreadBudget); err != nil {
		t.Fatal(err)
	}
	if got := count(); got != 2 {
		t.Fatalf("one-subscriber bump statements = %d, want 2 (member read + one authorization)\n%s", got, strings.Join(dump(), "\n"))
	}
}

// TestUnreadBumpConcurrencyPoint pins the shortcut's two-sided linearization:
// a subscription registered before the bump is delivered to and one removed
// before the bump is not.
func TestUnreadBumpConcurrencyPoint(t *testing.T) {
	hub, db, owner, roomID, _ := hubFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, _ := unreadSubscriber(t, hub, db, owner)

	if err := hub.PublishUnreadBump(roomID, unreadBudget); err != nil {
		t.Fatal(err)
	}
	expectBump(t, ctx, c, roomID)

	hub.mu.Lock()
	delete(c.subscriptions, unreadIdentifier)
	hub.indexRemoveLocked(c, unreadIdentifier, unreadSubscription(owner))
	hub.mu.Unlock()
	if err := hub.PublishUnreadBump(roomID, unreadBudget); err != nil {
		t.Fatal(err)
	}
	expectNone(t, c)

	// Re-subscribing restores delivery (the index is the only gate).
	hub.mu.Lock()
	c.subscriptions[unreadIdentifier] = unreadSubscription(owner)
	hub.indexAddLocked(c, unreadIdentifier, unreadSubscription(owner))
	hub.mu.Unlock()
	if err := hub.PublishUnreadBump(roomID, unreadBudget); err != nil {
		t.Fatal(err)
	}
	expectBump(t, ctx, c, roomID)
}

// TestUnreadBumpSnapshotBoundary pins the boundary the shortcut rests on,
// deterministically: the publication's recipient snapshot holds the hub read
// lock and a subscribe needs the write lock, so a subscription cannot land
// inside a snapshot — it is either entirely before it (and in it) or entirely
// after it. That is what makes "registered before the post" a subset of the
// snapshot and "registered after" disjoint from it; the churn test below only
// exercises the same invariant statistically, under -race.
func TestUnreadBumpSnapshotBoundary(t *testing.T) {
	hub, db, owner, roomID, _ := hubFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	token, err := db.StartSession(ctx, owner.ID, "boundary", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	// A client holding its subscription but not yet registered with the hub.
	c := simulateClient(t, owner, token, unreadIdentifier, unreadSubscription(owner))

	// Hold the lock a snapshot holds and try to land a subscribe inside it.
	hub.mu.RLock()
	registered := make(chan struct{})
	go func() {
		defer close(registered)
		hub.register(c)
	}()
	select {
	case <-registered:
		t.Fatal("a subscribe completed while the publication's snapshot lock was held")
	case <-time.After(50 * time.Millisecond):
	}
	hub.mu.RUnlock()
	<-registered

	// Registered before the next snapshot: delivered.
	if err := hub.PublishUnreadBump(roomID, unreadBudget); err != nil {
		t.Fatal(err)
	}
	expectBump(t, ctx, c, roomID)

	// Removed under the same lock: no longer a recipient.
	hub.mu.Lock()
	delete(c.subscriptions, unreadIdentifier)
	hub.indexRemoveLocked(c, unreadIdentifier, unreadSubscription(owner))
	hub.mu.Unlock()
	if err := hub.PublishUnreadBump(roomID, unreadBudget); err != nil {
		t.Fatal(err)
	}
	expectNone(t, c)
}

// TestUnreadBumpConcurrentChurn exercises the bump against concurrent
// subscribe/unsubscribe churn under -race: every frame any client observes
// must be a bump frame, and once the churn has stopped a final bump must reach
// every client that is still subscribed (the index and the client set stay
// consistent).
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
				if err := hub.PublishUnreadBump(roomID, unreadBudget); err != nil {
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
	if err := hub.PublishUnreadBump(roomID, unreadBudget); err != nil {
		t.Fatal(err)
	}
	for _, c := range clients {
		expectBump(t, ctx, c, roomID)
		expectNone(t, c)
	}
}
