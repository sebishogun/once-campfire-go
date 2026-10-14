package web

// Full public-chain parity for the ENGINE-49 precomposed head lane: the real
// application served through the production public composition
// (PublicCompression, the response cache, Deflate) on the owned loop, with the
// front's capture wrapper transparent to the lane and with the lane hidden by
// an opaque writer. Raw wire bytes are compared with Date masked, so the status
// line, the header block in order, the framing (Content-Length vs chunked) and
// the body must all match.
//
// The chain here is the production composition minus `forward`, which only
// edits request headers and is response-neutral (see aux_parity_test.go). A
// probe between the loop and the chain counts the precomposed emissions that
// reach the writer, so engagement is measured, not assumed.

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/basecamp/once-campfire-go/internal/database"
	"github.com/basecamp/once-campfire-go/internal/fastserve"
	"github.com/basecamp/once-campfire-go/internal/front"
	"github.com/basecamp/once-campfire-go/internal/rails"
)

// publicOpaqueWriter ends the precomposed-receiver walk while forwarding the
// plain ResponseWriter surface: the chain then runs its header-map path on the
// same loop.
type publicOpaqueWriter struct{ http.ResponseWriter }

func hidePrecomposed(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(publicOpaqueWriter{w}, r)
	})
}

// seamProbe sits between the loop and the public chain and counts the
// precomposed emissions by status, forwarding them to the loop. It is a
// precomposed receiver itself, so the chain's walk finds it exactly as it would
// find the loop.
type seamProbe struct {
	mu     sync.Mutex
	status map[int]int
}

func newSeamProbe() *seamProbe { return &seamProbe{status: map[int]int{}} }

func (p *seamProbe) record(status int) {
	p.mu.Lock()
	p.status[status]++
	p.mu.Unlock()
}

func (p *seamProbe) count(status int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status[status]
}

func (p *seamProbe) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&probeWriter{ResponseWriter: w, probe: p}, r)
	})
}

type probeWriter struct {
	http.ResponseWriter
	probe *seamProbe
}

func (w *probeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *probeWriter) WritePrecomposed(status int, head []byte, body [][]byte) error {
	w.probe.record(status)
	for {
		if receiver, ok := w.ResponseWriter.(interface {
			WritePrecomposed(int, []byte, [][]byte) error
		}); ok {
			return receiver.WritePrecomposed(status, head, body)
		}
		unwrapper, ok := w.ResponseWriter.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return nil
		}
		w.ResponseWriter = unwrapper.Unwrap()
	}
}

// publicChain is one side of the differential: the application, the probe that
// counted its emissions, and the loop address.
type publicChain struct {
	app   *Server
	probe *seamProbe
	addr  string
}

// testPublicChainPair builds two applications over one database — the public
// chain with the precomposed lane reachable and with it hidden — each served by
// a real owned loop. cacheSize is the front response cache's capacity: a
// positive value exercises the miss path, zero the bypass path.
func testPublicChainPair(t *testing.T, cacheSize int64) (on, off publicChain, cookie *http.Cookie, user database.User) {
	t.Helper()
	t.Setenv("CAMPFIRE_FROZEN_TIME", "2026-01-02T03:04:05Z")
	t.Setenv("CAMPFIRE_RECORDED_PIECES", "on")
	t.Setenv("CAMPFIRE_PRECOMPOSED_FRAMING", "on")
	t.Setenv("CAMPFIRE_REQUEST_ARENA", "on")
	// The whole-response cache would serve the second request of a case from
	// the stored body on the map path, hiding the precomposed lane; disabling
	// it forces a fresh render so the lane is exercised deterministically.
	t.Setenv("CAMPFIRE_RESPONSE_CACHE_MB", "0")
	root := t.TempDir()
	db, err := database.Open(filepath.Join(root, "test.sqlite3"), 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	secrets, err := rails.NewSecrets("recorded-pieces")
	if err != nil {
		t.Fatal(err)
	}
	onApp, err := New(db, secrets, false, filepath.Join(root, "test.sqlite3"), root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(onApp.Close)
	offApp, err := New(db, secrets, false, filepath.Join(root, "test.sqlite3"), root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(offApp.Close)
	user, err = db.Setup(context.Background(), "Owner", "owner@test", "digest")
	if err != nil {
		t.Fatal(err)
	}
	token, err := db.StartSession(context.Background(), user.ID, "test", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	signed, err := secrets.SignCookie("session_token", token, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	cookie = &http.Cookie{Name: "session_token", Value: rails.EscapeCookie(signed)}

	cfg := front.Config{Gzip: true, CompressionJitter: 32, LogRequests: false, CacheSize: cacheSize, MaxCacheItemSize: cacheSize}
	compose := func(app http.Handler) http.Handler {
		cache := front.NewCache(cfg.CacheSize, cfg.MaxCacheItemSize)
		cache.FixedRoutes = true
		return front.PublicCompression(cache.Handler(front.Deflate(app)), cfg)
	}
	on = publicChain{app: onApp, probe: newSeamProbe()}
	on.addr = servePublicLoop(t, on.probe.wrap(compose(onApp)))
	off = publicChain{app: offApp, probe: newSeamProbe()}
	off.addr = servePublicLoop(t, off.probe.wrap(compose(hidePrecomposed(offApp))))
	return
}

// servePublicLoop runs one handler on a private owned loop and returns its
// address.
func servePublicLoop(t *testing.T, handler http.Handler) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := fastserve.New(handler)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})
	go srv.Serve(listener)
	return listener.Addr().String()
}

// TestPublicChainPrecomposedParity is the strategy-1 contract on the miss path:
// with the front capture wrapper transparent, the precomposed lane must emit
// the same bytes as the map path. Every case runs twice (the first pass fills
// the piece and variant caches, the second is the lane), the lane is proved
// engaged by the probe (a 200 and a 304 emission on the on-side, none on the
// hidden side), and the application's assembly counter confirms the gzip render
// stopped taking the map path's compressed branch.
func TestPublicChainPrecomposedParity(t *testing.T) {
	on, off, cookie, user := testPublicChainPair(t, 64<<20)
	ctx := context.Background()
	rooms, err := on.app.DB.Rooms(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	room := rooms[0]
	var anchor int64
	for i := 0; i < 3; i++ {
		message, err := on.app.DB.CreateMessage(ctx, user.ID, room.ID, "pc-"+strconv.Itoa(i), "<p>pc seed</p>", "pc seed")
		if err != nil {
			t.Fatal(err)
		}
		if anchor == 0 {
			anchor = message.ID
		}
	}
	path := "/rooms/" + strconv.FormatInt(room.ID, 10)
	messagesPath := path + "/messages"
	lastRoom := "last_room=" + strconv.FormatInt(room.ID, 10)

	// Warm both encodings of the room page on both chains, then prove the lane
	// is the active writer on the on-side: a fresh gzip render must emit a
	// precomposed 200 and must not run the map path's compressed assembly.
	for _, addr := range []string{on.addr, off.addr} {
		rawFetch(t, addr, "campfire.test", "GET", path, "gzip", "", cookie, lastRoom)
		rawFetch(t, addr, "campfire.test", "GET", path, "identity", "", cookie, lastRoom)
	}
	assemblies := on.app.recordedAssemblies.Load()
	etag := ""
	raw, status := rawFetch(t, on.addr, "campfire.test", "GET", path, "gzip", "", cookie, lastRoom)
	if status != 200 {
		t.Fatalf("warm gzip: status %d", status)
	}
	if on.probe.count(200) == 0 {
		t.Fatal("the precomposed lane was not engaged on the public chain")
	}
	if got := on.app.recordedAssemblies.Load(); got != assemblies {
		t.Fatalf("the framed gzip render ran the map path's assembly: %d", got-assemblies)
	}
	for _, line := range bytes.Split(maskedHead(raw), []byte("\r\n")) {
		if bytes.HasPrefix(line, []byte("Etag: ")) {
			etag = string(bytes.TrimPrefix(line, []byte("Etag: ")))
		}
	}
	if etag == "" {
		t.Fatal("no ETag on the warm response")
	}
	if off.probe.count(200) != 0 {
		t.Fatal("the hidden-lane chain emitted a precomposed response")
	}

	cases := []struct {
		label, method, path, accept, ifNoneMatch string
	}{
		{"gzip", "GET", path, "gzip", ""},
		{"identity", "GET", path, "identity", ""},
		{"absent", "GET", path, "", ""},
		{"wildcard", "GET", path, "*", ""},
		{"gzip refused", "GET", path, "gzip;q=0", ""},
		{"head gzip", "HEAD", path, "gzip", ""},
		{"head identity", "HEAD", path, "identity", ""},
		{"messages gzip", "GET", messagesPath, "gzip", ""},
		{"messages identity", "GET", messagesPath, "identity", ""},
		{"fresh gzip", "GET", path, "gzip", etag},
		{"fresh identity", "GET", path, "identity", etag},
		{"anchor gzip", "GET", path + "/@" + strconv.FormatInt(anchor, 10), "gzip", ""},
	}
	for round := 0; round < 2; round++ {
		for _, c := range cases {
			rawOn, statusOn := rawFetch(t, on.addr, "campfire.test", c.method, c.path, c.accept, c.ifNoneMatch, cookie, lastRoom)
			rawOff, statusOff := rawFetch(t, off.addr, "campfire.test", c.method, c.path, c.accept, c.ifNoneMatch, cookie, lastRoom)
			if statusOn != statusOff {
				t.Fatalf("%s/%d: status %d vs %d", c.label, round, statusOn, statusOff)
			}
			a, b := maskDate(rawOn), maskDate(rawOff)
			if !bytes.Equal(a, b) {
				at := 0
				for at < len(a) && at < len(b) && a[at] == b[at] {
					at++
				}
				os.WriteFile("/tmp/opencode/pc."+c.label+".on", a, 0o644)
				os.WriteFile("/tmp/opencode/pc."+c.label+".off", b, 0o644)
				t.Fatalf("%s/%d: wire differs at offset %d\n--- lane ---\n%q\n--- map ---\n%q", c.label, round, at, a[max(0, at-80):min(len(a), at+80)], b[max(0, at-80):min(len(b), at+80)])
			}
			t.Logf("%s/%d OK (%d bytes)", c.label, round, len(rawOn))
		}
	}
	// The lane served the 200s and the conditional 304s on the on-side; the
	// hidden chain served none.
	if on.probe.count(200) == 0 || on.probe.count(304) == 0 {
		t.Fatalf("lane emissions: 200=%d 304=%d", on.probe.count(200), on.probe.count(304))
	}
	if off.probe.count(200) != 0 || off.probe.count(304) != 0 {
		t.Fatalf("hidden chain emissions: 200=%d 304=%d", off.probe.count(200), off.probe.count(304))
	}
}

// TestPublicChainBypassMarkerParity pins the bypass path's marker: with the
// front response cache disabled every public request bypasses it, and the
// application still frames the recorded page, so the front's X-Cache marker
// must reach the wire through the seam exactly as the map path emits it.
func TestPublicChainBypassMarkerParity(t *testing.T) {
	on, off, cookie, user := testPublicChainPair(t, 0)
	ctx := context.Background()
	rooms, err := on.app.DB.Rooms(ctx, user.ID)
	if err != nil {
		t.Fatal(err)
	}
	room := rooms[0]
	if _, err := on.app.DB.CreateMessage(ctx, user.ID, room.ID, "bypass", "<p>bypass</p>", "bypass"); err != nil {
		t.Fatal(err)
	}
	path := "/rooms/" + strconv.FormatInt(room.ID, 10)
	lastRoom := "last_room=" + strconv.FormatInt(room.ID, 10)

	for round := 0; round < 2; round++ {
		rawOn, statusOn := rawFetch(t, on.addr, "campfire.test", "GET", path, "gzip", "", cookie, lastRoom)
		rawOff, statusOff := rawFetch(t, off.addr, "campfire.test", "GET", path, "gzip", "", cookie, lastRoom)
		if statusOn != statusOff {
			t.Fatalf("round %d: status %d vs %d", round, statusOn, statusOff)
		}
		if !bytes.Contains(rawOn, []byte("X-Cache: bypass\r\n")) {
			t.Fatalf("round %d: bypassed lane response lacks the X-Cache marker:\n%s", round, maskedHead(rawOn))
		}
		if !bytes.Equal(maskDate(rawOn), maskDate(rawOff)) {
			t.Fatalf("round %d: bypassed lane vs map mismatch\n--- lane ---\n%s\n--- map ---\n%s", round, maskDate(rawOn), maskDate(rawOff))
		}
	}
	if on.probe.count(200) == 0 {
		t.Fatal("the bypassed public chain did not frame the recorded page")
	}
	if off.probe.count(200) != 0 {
		t.Fatal("the hidden-lane chain emitted a precomposed response")
	}
}

// maskedHead returns the head block of a raw response.
func maskedHead(raw []byte) []byte {
	head, _, _ := bytes.Cut(raw, []byte("\r\n\r\n"))
	return head
}
