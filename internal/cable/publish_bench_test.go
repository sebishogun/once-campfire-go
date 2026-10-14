// Publish-path micro-benchmark (ENGINE-40b): one hub.Publish to N distinct
// sessions (one per simulated client) with a warm authorization cache and
// drained client queues, so the measurement is the steady-state fan-out cost:
// recipient snapshot, per-recipient authorization, frame reuse, per-recipient
// channel delivery. Run with a fixed benchmark CPU set, e.g.
//
//	taskset -c 16-31 nice -n 19 env GOMAXPROCS=4 go test -run '^$' -bench BenchmarkPublishFanout -benchmem -count=5 ./internal/cable/
package cable

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/basecamp/once-campfire-go/internal/database"
)

// oldUnreadFanout is the pre-Lever-5 shape of the unread bump, kept for the
// benchmark A/B: read every room member, then one stream publish per member
// (each publish snapshots that member's stream, authorizes and sends).
func oldUnreadFanout(hub *Hub, ctx context.Context, room int64) {
	members, err := hub.db.RoomMemberIDs(ctx, room)
	if err != nil {
		return
	}
	payload := map[string]any{"roomId": room}
	for _, id := range members {
		hub.PublishStream(ctx, fmt.Sprintf("user_%d_unreads", id), payload)
	}
}

// BenchmarkUnreadBumpFanout compares the Lever-5 bump (a room-member read, one
// bulk authorization, one shared frame) with the shape it replaced, over a room
// with 100 members and 1 or 100 of them subscribed. Run with:
//
//	taskset -c 16-31 nice -n 19 env GOMAXPROCS=4 go test -run '^$' -bench BenchmarkUnreadBumpFanout -benchmem -count=5 ./internal/cable/
func BenchmarkUnreadBumpFanout(b *testing.B) {
	for _, subs := range []int{1, 100} {
		b.Run("subs="+strconv.Itoa(subs), func(b *testing.B) {
			hub, db, owner, roomID, _ := hubFixture(b)
			ctx := context.Background()
			users := []database.User{owner}
			for i := 1; i < 100; i++ {
				u, err := db.CreateUser(ctx, fmt.Sprintf("Member%d", i), fmt.Sprintf("m%d@test", i), "digest", "", 0, nil)
				if err != nil {
					b.Fatal(err)
				}
				users = append(users, u)
			}
			clients := make([]*client, 0, subs)
			for i := 0; i < subs; i++ {
				c, _ := unreadSubscriber(b, hub, db, users[i])
				clients = append(clients, c)
			}
			drain := func() {
				for _, c := range clients {
					c.q.clear()
				}
			}
			// Warm the frame and authorization paths so the measurement is
			// steady state.
			hub.PublishUnreadBump(roomID, unreadBudget)
			oldUnreadFanout(hub, ctx, roomID)
			drain()
			b.Run("bump", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					drain()
					hub.PublishUnreadBump(roomID, unreadBudget)
				}
			})
			b.Run("member-fanout", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					drain()
					oldUnreadFanout(hub, ctx, roomID)
				}
			})
		})
	}
}

// BenchmarkUnreadBumpUnrelatedSubscribers pins the candidate strategy's
// scaling: a fixed two-member target room, one subscribed member, and 0/100/
// 10,000 unread subscribers whose users are members of no room the post
// touches. The bump's cost must be flat in that last number — an unrelated
// subscriber contributes no token, no row and no lock-held work — while the
// pre-Lever-5 shape would have published once per unrelated subscriber too.
// Run with the same pinned-CPU command as BenchmarkUnreadBumpFanout.
func BenchmarkUnreadBumpUnrelatedSubscribers(b *testing.B) {
	for _, straysN := range []int{0, 100, 10000} {
		b.Run("strays="+strconv.Itoa(straysN), func(b *testing.B) {
			hub, db, owner, _, _ := hubFixture(b)
			ctx := context.Background()
			member, err := db.CreateUser(ctx, "Member", "member@test", "digest", "", 0, nil)
			if err != nil {
				b.Fatal(err)
			}
			pair, err := db.CreateRoom(ctx, owner.ID, "Rooms::Closed", "Pair", []int64{owner.ID, member.ID})
			if err != nil {
				b.Fatal(err)
			}
			strays := make([]*client, 0, straysN)
			for i := 0; i < straysN; i++ {
				strays = append(strays, strayUnreadSubscriber(hub, 1_000_000+int64(i), func() {}))
			}
			memberC, _ := unreadSubscriber(b, hub, db, member)
			hub.PublishUnreadBump(pair.ID, unreadBudget)
			memberC.q.clear()
			_ = strays // never recipients: only the member's queue can fill
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				memberC.q.clear()
				hub.PublishUnreadBump(pair.ID, unreadBudget)
			}
		})
	}
}

func BenchmarkPublishFanout(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run("clients="+strconv.Itoa(n), func(b *testing.B) {
			hub, db, user, roomID, _ := hubFixture(b)
			ctx := context.Background()
			identifier := `{"channel":"RoomMessagesChannel","room_id":` + strconv.FormatInt(roomID, 10) + `}`
			sub := subscription{Channel: "RoomMessagesChannel", Room: roomID}
			var clients []*client
			for i := 0; i < n; i++ {
				// One real session per client: the authorization query and
				// the cache key are per (room, token).
				token, err := db.StartSession(ctx, user.ID, "bench", "127.0.0.1")
				if err != nil {
					b.Fatal(err)
				}
				c := simulateClient(b, user, token, identifier, sub)
				clients = append(clients, c)
				hub.register(c)
			}
			markup := "<turbo-stream action=\"append\"><template>" + strings.Repeat("payload ", 50) + "</template></turbo-stream>"
			// drain empties every client queue so sendFrame always takes the
			// send arm and the 256-frame slow-client bound is never hit.
			drain := func() {
				for _, c := range clients {
					c.q.clear()
				}
			}
			hub.Publish(ctx, roomID, markup)
			drain()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				drain()
				hub.Publish(ctx, roomID, markup)
			}
		})
	}
}
