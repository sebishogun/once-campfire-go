package web

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/basecamp/once-campfire-go/internal/database"
	"github.com/basecamp/once-campfire-go/internal/front"
)

// This file is the direct-replay lever's contract (response_cache.go's
// replayCached/serveCached304). A whole-response-cache hit used to fall back
// to the map path — a header-value copy per stored header, the loop's header
// snapshot and sort, and a full copy of the stored body into the loop's write
// buffer. The replay lane stores the head block the fresh response emitted
// (with Content-Length and the front's X-Cache marker baked in) alongside the
// body and emits a hit through the same precomposed seam the fresh recorded
// path uses. These tests pin the wire bytes: a hit must equal a fresh render
// of the same request, through the owned loop and through the public front
// chain, for 200, HEAD and 304, identity and gzip.

// publicReplayChain composes the public listener's front chain (the response
// cache's capture plus the public compression policy) without front.Serve's
// listeners, so a test can serve it on the owned loop and read the raw wire.
func publicReplayChain(app http.Handler) http.Handler {
	config := front.FromLookup(func(string) (string, bool) { return "", false })
	config.SkipDeflate = true
	return front.PublicCompression(front.NewCache(config.CacheSize, config.MaxCacheItemSize).Handler(app), config)
}

// replayShape is one request shape the parity contract covers: the route, the
// negotiated encoding and the stable cookies a real browser repeats.
type replayShape struct {
	name   string
	path   string
	accept string
	extra  []string
}

func replayShapes(roomID string) []replayShape {
	return []replayShape{
		// The room route sets last_room when it is absent, so this shape is a
		// map-path response; the hit must reproduce that map head.
		{"room-identity", "/rooms/" + roomID, "identity", nil},
		{"room-gzip", "/rooms/" + roomID, "gzip", nil},
		// A returning browser repeats last_room, so no Set-Cookie is added and
		// the recorded response frames: this is the replay lane's hot shape.
		{"room-framed-identity", "/rooms/" + roomID, "identity", []string{"last_room=" + roomID}},
		{"room-framed-gzip", "/rooms/" + roomID, "gzip", []string{"last_room=" + roomID}},
		{"messages-identity", "/rooms/" + roomID + "/messages", "identity", nil},
		{"messages-gzip", "/rooms/" + roomID + "/messages", "gzip", nil},
		{"search-identity", "/searches?q=replay", "identity", nil},
		{"search-gzip", "/searches?q=replay", "gzip", nil},
	}
}

// replayEngages reports whether a shape's hit takes the precomposed replay
// lane. The room's non-framed shapes take the map path by design (the
// last_room Set-Cookie must reach the head); the framed room, messages and
// search shapes replay for both GET and HEAD (the recorded HEAD is composed
// on the map path, but its head is the same entity head the GET stored, and
// the receiver suppresses the body). A HEAD 304 stays on the map path because
// the fresh framed 304 branch excludes HEAD.
func replayEngages(shape string) bool {
	return shape != "room-identity" && shape != "room-gzip"
}

// TestResponseCacheReplayWireParity is the direct-replay lever's byte-identity
// contract: for every shape, the cache-on hit and the cache-off fresh render
// are byte-identical (Date masked) on the raw wire, through the owned loop and
// through the public front chain. The hit counter must move for every hit and
// the replay counter must move wherever the lane is expected to engage, so a
// run that silently took the map path cannot pass as a replay.
func TestResponseCacheReplayWireParity(t *testing.T) {
	chains := []struct {
		name string
		mk   func(http.Handler) http.Handler
	}{
		{"owned-loop", front.Deflate},
		{"public-chain", publicReplayChain},
	}
	for _, chain := range chains {
		t.Run(chain.name, func(t *testing.T) {
			on, off, cookie, user := testResponseCacheApps(t)
			roomID := seedReplayRoom(t, on, user)
			_, onAddr := serveLoop(t, chain.mk(on))
			_, offAddr := serveLoop(t, chain.mk(off))

			for _, shape := range replayShapes(roomID) {
				t.Run(shape.name, func(t *testing.T) {
					fill, status := rawFetch(t, onAddr, "campfire.test", "GET", shape.path, shape.accept, "", cookie, shape.extra...)
					if status != 200 {
						t.Fatalf("fill: status %d\n%s", status, fill)
					}
					etag := headerValue(fill, "Etag")
					if etag == "" {
						t.Fatalf("fill carries no validator:\n%s", fill)
					}
					// 200 and HEAD: the fresh render (cache disabled) and the
					// hit (cache enabled) must agree byte for byte.
					for _, method := range []string{"GET", "HEAD"} {
						fresh, freshStatus := rawFetch(t, offAddr, "campfire.test", method, shape.path, shape.accept, "", cookie, shape.extra...)
						hits0, _ := on.responses.counters()
						replays0 := on.responseReplays.Load()
						hit, hitStatus := rawFetch(t, onAddr, "campfire.test", method, shape.path, shape.accept, "", cookie, shape.extra...)
						hits1, misses1 := on.responses.counters()
						replays1 := on.responseReplays.Load()
						if freshStatus != 200 || hitStatus != 200 {
							t.Fatalf("%s: fresh %d hit %d", method, freshStatus, hitStatus)
						}
						if hits1 <= hits0 {
							t.Fatalf("%s: the request did not hit the response cache (hits %d -> %d, misses %d)", method, hits0, hits1, misses1)
						}
						if !bytes.Equal(maskDate(fresh), maskDate(hit)) {
							t.Fatalf("%s: hit differs from the fresh render:\nFRESH:\n%s\nHIT:\n%s", method, maskDate(fresh), maskDate(hit))
						}
						if replayEngages(shape.name) && replays1 <= replays0 {
							t.Fatalf("%s: the hit did not take the replay lane (replays %d -> %d)", method, replays0, replays1)
						}
					}
					// 304: the hit's conditional response must be the fresh
					// 304 byte for byte, bodyless.
					fresh304, freshStatus := rawFetch(t, offAddr, "campfire.test", "GET", shape.path, shape.accept, etag, cookie, shape.extra...)
					hit304, hitStatus := rawFetch(t, onAddr, "campfire.test", "GET", shape.path, shape.accept, etag, cookie, shape.extra...)
					if freshStatus != http.StatusNotModified || hitStatus != http.StatusNotModified {
						t.Fatalf("304: fresh %d hit %d\nFRESH:\n%s\nHIT:\n%s", freshStatus, hitStatus, fresh304, hit304)
					}
					if !bytes.Equal(maskDate(fresh304), maskDate(hit304)) {
						t.Fatalf("304: hit differs from the fresh render:\nFRESH:\n%s\nHIT:\n%s", maskDate(fresh304), maskDate(hit304))
					}
				})
			}
		})
	}
}

// TestResponseCacheHit304ReproducesFreshHead is the focused regression for the
// conditional-hit head. On the pre-lever tree a hit's 304 carried the stored
// entity headers (Cache-Control: max-age=0 and the ETag for the recorded
// routes; Content-Encoding and Vary for the messages page), while the fresh
// 304 does not: the recorded routes answer from finish's notModified
// "no-cache" fallback, and the messages page answers from its validator before
// rendering. This test fails on that tree and passes with the flavor-aware
// 304 head.
func TestResponseCacheHit304ReproducesFreshHead(t *testing.T) {
	on, off, cookie, user := testResponseCacheApps(t)
	roomID := seedReplayRoom(t, on, user)
	_, onAddr := serveLoop(t, front.Deflate(on))
	_, offAddr := serveLoop(t, front.Deflate(off))

	for _, shape := range []replayShape{
		{"messages-gzip", "/rooms/" + roomID + "/messages", "gzip", nil},
		{"room-gzip", "/rooms/" + roomID, "gzip", nil},
		{"room-framed-gzip", "/rooms/" + roomID, "gzip", []string{"last_room=" + roomID}},
		{"search-gzip", "/searches?q=replay", "gzip", nil},
	} {
		t.Run(shape.name, func(t *testing.T) {
			fill, _ := rawFetch(t, onAddr, "campfire.test", "GET", shape.path, shape.accept, "", cookie, shape.extra...)
			etag := headerValue(fill, "Etag")
			if etag == "" {
				t.Fatalf("fill carries no validator:\n%s", fill)
			}
			fresh, freshStatus := rawFetch(t, offAddr, "campfire.test", "GET", shape.path, shape.accept, etag, cookie, shape.extra...)
			hits0, _ := on.responses.counters()
			hit, hitStatus := rawFetch(t, onAddr, "campfire.test", "GET", shape.path, shape.accept, etag, cookie, shape.extra...)
			hits1, _ := on.responses.counters()
			if freshStatus != http.StatusNotModified || hitStatus != http.StatusNotModified {
				t.Fatalf("status: fresh %d hit %d", freshStatus, hitStatus)
			}
			if hits1 <= hits0 {
				t.Fatalf("the conditional request did not hit the response cache (hits %d -> %d)", hits0, hits1)
			}
			if !bytes.Equal(maskDate(fresh), maskDate(hit)) {
				t.Fatalf("304 differs from the fresh render:\nFRESH:\n%s\nHIT:\n%s", maskDate(fresh), maskDate(hit))
			}
		})
	}
}

// TestResponseCacheReplayEngages pins the lane's engagement on the hot shape:
// a returning browser's messages-page hit is emitted through the precomposed
// seam, and its 200 and 304 both move the replay counter. The non-framed room
// shape (a last_room Set-Cookie must reach the head) deliberately does not.
func TestResponseCacheReplayEngages(t *testing.T) {
	app, _, cookie, user := testResponseCacheApps(t)
	roomID := seedReplayRoom(t, app, user)
	_, addr := serveLoop(t, front.Deflate(app))
	path := "/rooms/" + roomID + "/messages"

	fill, status := rawFetch(t, addr, "campfire.test", "GET", path, "gzip", "", cookie)
	if status != 200 {
		t.Fatalf("fill: status %d", status)
	}
	etag := headerValue(fill, "Etag")
	for _, c := range []struct {
		label       string
		ifNoneMatch string
	}{
		{"200", ""},
		{"304", etag},
	} {
		replays0 := app.responseReplays.Load()
		hits0, _ := app.responses.counters()
		_, status := rawFetch(t, addr, "campfire.test", "GET", path, "gzip", c.ifNoneMatch, cookie)
		replays1 := app.responseReplays.Load()
		hits1, _ := app.responses.counters()
		if status != 200 && status != http.StatusNotModified {
			t.Fatalf("%s: status %d", c.label, status)
		}
		if hits1 <= hits0 {
			t.Fatalf("%s: no cache hit (hits %d -> %d)", c.label, hits0, hits1)
		}
		if replays1 <= replays0 {
			t.Fatalf("%s: the hit did not take the replay lane (replays %d -> %d)", c.label, replays0, replays1)
		}
	}
	// The room's non-framed shape keeps the map path: its Set-Cookie must be
	// emitted, so the replay lane refuses it.
	roomPath := "/rooms/" + roomID
	rawFetch(t, addr, "campfire.test", "GET", roomPath, "gzip", "", cookie)
	replays0 := app.responseReplays.Load()
	rawFetch(t, addr, "campfire.test", "GET", roomPath, "gzip", "", cookie)
	if replays := app.responseReplays.Load(); replays != replays0 {
		t.Fatalf("the Set-Cookie room shape took the replay lane (replays %d -> %d)", replays0, replays)
	}
}

// TestResponseCacheReplayDisabledBaseline is the rollback comparison: with the
// response cache disabled the rollback switch changes nothing on the wire and
// the replay lane never engages, and the enabled hit is byte-identical to that
// disabled baseline.
func TestResponseCacheReplayDisabledBaseline(t *testing.T) {
	on, off, cookie, user := testResponseCacheApps(t)
	roomID := seedReplayRoom(t, on, user)
	_, onAddr := serveLoop(t, front.Deflate(on))
	_, offAddr := serveLoop(t, front.Deflate(off))
	path := "/rooms/" + roomID + "/messages"

	first, _ := rawFetch(t, offAddr, "campfire.test", "GET", path, "gzip", "", cookie)
	second, _ := rawFetch(t, offAddr, "campfire.test", "GET", path, "gzip", "", cookie)
	if !bytes.Equal(maskDate(first), maskDate(second)) {
		t.Fatal("repeated renders differ with the response cache disabled")
	}
	if hits, _ := off.responses.counters(); hits != 0 {
		t.Fatalf("budget-zero cache reported %d hits", hits)
	}
	if replays := off.responseReplays.Load(); replays != 0 {
		t.Fatalf("budget-zero cache reported %d replays", replays)
	}
	rawFetch(t, onAddr, "campfire.test", "GET", path, "gzip", "", cookie)
	hit, _ := rawFetch(t, onAddr, "campfire.test", "GET", path, "gzip", "", cookie)
	if !bytes.Equal(maskDate(first), maskDate(hit)) {
		t.Fatalf("the hit differs from the cache-disabled baseline:\nBASELINE:\n%s\nHIT:\n%s", maskDate(first), maskDate(hit))
	}
}

// seedReplayRoom creates one message in the user's first room so every shape
// renders a populated page, and returns the room id as a path segment.
func seedReplayRoom(t *testing.T, app *Server, user database.User) string {
	t.Helper()
	rooms, err := app.DB.Rooms(context.Background(), user.ID)
	if err != nil || len(rooms) == 0 {
		t.Fatalf("fixture has no rooms: %v", err)
	}
	if _, err := app.DB.CreateMessage(context.Background(), user.ID, rooms[0].ID, "", "<p>replay seed message</p>", "replay seed message"); err != nil {
		t.Fatal(err)
	}
	return strconv.FormatInt(rooms[0].ID, 10)
}
