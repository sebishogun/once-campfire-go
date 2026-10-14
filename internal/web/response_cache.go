package web

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/basecamp/once-campfire-go/internal/front"
)

// The whole-response cache (upstream e3a1309, raised from budget zero): the
// completed HTML responses of the room and messages-page routes, keyed by the
// observed database generation (PRAGMA data_version on a pinned reader — the
// same namespace fragmentKey uses), the authenticated user and every request
// input that can change the bytes. A warm request re-checks the session and
// the generation, then serves the stored wire body: no message reads, no
// template execution, no per-request compression or assembly, no validator
// computation. This is the shape of the C reference's "64 MiB
// response-body cache" hits (authorization re-checked, external SQLite
// commits observed), which is what wins its warm reads.
//
// The store copies the exact wire bytes the fresh render produced (the
// assembled gzip member for gzip clients, the joined pieces for identity
// clients), so a hit is byte-identical to a fresh render by construction; the
// stored entity headers (Content-Type, ETag, Last-Modified, Cache-Control,
// Vary, Content-Encoding) are the same ones the fresh response emits. A
// session flash is per-request state and never cached. The finer-grained
// engine caches (pieces, refs, search) still serve the miss path under
// writes; a committed write moves the generation and both the old entries and
// the next fill age out exactly as the sidebar gate does (single-process
// observation limit, README.md). The entry cost gate and LRU are the same
// shape as the fragment cache's. The sidebar is deliberately absent (see
// beginResponseCache): its own whole-page cache already serves the warm shape.
// Search takes part in the round; its own result cache still serves the miss
// path, and the response key covers the query and the generation so a purge or
// a write moves the served bytes.

type cachedResponse struct {
	digest  [32]byte
	key     []byte
	version uint64
	body    []byte
	header  http.Header
	cost    int
}
type responseCache struct {
	mu           sync.Mutex
	entries      map[[32]byte]*list.Element
	order        list.List
	limit, size  int
	version      uint64
	hits, misses uint64
}
type responseRound struct {
	version          uint64
	user             int64
	keyed            bool
	gzip, hit, flash bool
}

func newResponseCache(limit int) *responseCache {
	return &responseCache{limit: limit, entries: make(map[[32]byte]*list.Element)}
}

// get looks a completed response up by the digest of its exact key bytes. A
// digest hit is confirmed against the stored key bytes, so a hash collision
// reads as a miss rather than another request's page.
func (c *responseCache) get(digest [32]byte, key []byte, version uint64) *cachedResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	if version < c.version {
		c.misses++
		return nil
	}
	if version != c.version {
		c.entries = make(map[[32]byte]*list.Element)
		c.order.Init()
		c.size = 0
		c.version = version
	}
	element := c.entries[digest]
	if element == nil {
		c.misses++
		return nil
	}
	entry := element.Value.(*cachedResponse)
	if !bytes.Equal(entry.key, key) {
		c.misses++
		return nil
	}
	c.order.MoveToFront(element)
	c.hits++
	return entry
}
func (c *responseCache) put(entry *cachedResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry.version != c.version || len(entry.key) > 8192 || entry.cost > c.limit/8 {
		return
	}
	if c.entries[entry.digest] != nil {
		return
	}
	for c.size+entry.cost > c.limit && c.order.Len() > 0 {
		oldest := c.order.Back()
		old := oldest.Value.(*cachedResponse)
		delete(c.entries, old.digest)
		c.order.Remove(oldest)
		c.size -= old.cost
	}
	c.entries[entry.digest] = c.order.PushFront(entry)
	c.size += entry.cost
}
func (c *responseCache) counters() (hits, misses uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses
}

// beginResponseCache starts a caching round for a request when the route is
// one the cache covers and the encoding is one it can store. It runs after
// the browser session is attached (for the flash check) and before any read.
// The generation is the one ServeHTTP observed moments earlier (the same
// value responseHit re-checks against a fresh read, so a commit between the
// two observations misses the cache rather than serving stale bytes); it
// adds no SQLite read of its own. The sidebar is deliberately absent: its
// own whole-page cache already serves warm requests, and a round here would
// only add the generation read.
func (s *Server) beginResponseCache(r *http.Request) {
	info := requestMetadata(r.Context())
	if info == nil {
		return
	}
	if s.responses.limit == 0 {
		// Budget zero disables storage; every lookup would miss, so the
		// round (and its per-request work) is skipped entirely — the rollback
		// switch costs nothing and every request behaves exactly as before
		// the cache existed.
		return
	}
	if (r.Method != "GET" && r.Method != "HEAD") || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		return
	}
	route, _, err := recognize(r.Method, r.URL.EscapedPath())
	if err != nil || route == nil {
		return
	}
	switch route.Endpoint {
	// The sidebar is absent: its own whole-page cache already serves warm
	// requests. Search takes part: the round stores the search page's wire
	// bytes, and the key covers the query and the observed generation, so a
	// POST/DELETE that purges the result cache (and commits) moves the
	// generation and the next GET misses. The result cache still serves the
	// miss path.
	case "rooms#show", "messages#index", "searches#index":
	default:
		return
	}
	encoding := front.ResponseEncoding(r.Header.Get("Accept-Encoding"))
	if encoding == "" {
		return
	}
	state := browserState(r)
	state.load()
	info.response = &responseRound{version: info.databaseVersion, gzip: encoding == "gzip", flash: state.values["flash"] != nil}
}

// responseHit looks the completed response up. The key covers everything the
// rendered bytes can depend on: the observed generation, the authenticated
// user, the request URI and form, the identity headers (cookie, accept,
// turbo-frame, user agent, origin, X-Requested-With), the negotiated encoding
// and the process revision. Auth has already re-checked the session in the
// auth middleware; a fresh generation read here observes any commit since the
// round began.
func (s *Server) responseHit(r *http.Request) *cachedResponse {
	info := requestMetadata(r.Context())
	if info == nil || info.response == nil {
		return nil
	}
	round := info.response
	if round.flash || round.user == 0 {
		return nil
	}
	version, err := s.DB.ResponseVersion(r.Context())
	if err != nil || version != round.version {
		return nil
	}
	// The key is built into a stack buffer and hashed in place: the map key is
	// the digest, and a hit is confirmed against the entry's stored key bytes.
	// A lookup therefore allocates nothing; only an admission copies the key.
	var buf [1536]byte
	key := appendResponseCacheKey(buf[:0], info, r, round, version)
	if len(key) > 8192 {
		return nil
	}
	round.keyed = true
	entry := s.responses.get(sha256.Sum256(key), key, version)
	round.hit = entry != nil
	return entry
}

// appendResponseCacheKey appends the length-prefixed cache key to b and
// returns it: every input is length-prefixed, so the key cannot alias
// different inputs. It replaces the original json.Marshal (which spent ~5% of
// a warm room route's samples) and then the string-returning builder (which
// allocated once per request); the caller hashes the returned bytes into a
// [32]byte map key, so the per-request path allocates nothing.
func appendResponseCacheKey(b []byte, info *requestInfo, r *http.Request, round *responseRound, version uint64) []byte {
	field := func(value string) {
		b = strconv.AppendInt(b, int64(len(value)), 10)
		b = append(b, ':')
		b = append(b, value...)
		b = append(b, '|')
	}
	field(info.host)
	field(info.origin)
	field(r.RequestURI)
	if r.RequestURI == "" {
		// A server request always carries the raw request-target, and the
		// parsed URL is derived from it, so the normalized reconstruction is
		// redundant then. When it is absent (a synthesized request), cover the
		// parsed parts instead. r.URL.RequestURI() would append the same bytes
		// but allocates a string per call.
		field(r.URL.EscapedPath())
		field(r.URL.RawQuery)
	}
	if len(r.Form) > 0 {
		// The form is written in sorted key order so the key is independent of
		// map iteration order. The keys are sorted in a stack array; only a
		// form with more than 16 distinct keys (never an admitted GET) falls
		// back to a heap slice. sort.Strings would force the stack array
		// through an interface and escape, so the insertion sort is inline.
		var fixed [16]string
		n := 0
		overflow := false
		for key := range r.Form {
			if n == len(fixed) {
				overflow = true
				break
			}
			i := n
			for i > 0 && fixed[i-1] > key {
				fixed[i] = fixed[i-1]
				i--
			}
			fixed[i] = key
			n++
		}
		if overflow {
			keys := make([]string, 0, len(r.Form))
			for key := range r.Form {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				for _, value := range r.Form[key] {
					b = append(b, '&')
					field(key)
					field(value)
				}
			}
		} else {
			for i := 0; i < n; i++ {
				key := fixed[i]
				for _, value := range r.Form[key] {
					b = append(b, '&')
					field(key)
					field(value)
				}
			}
		}
	}
	b = strconv.AppendInt(b, round.user, 10)
	b = append(b, '|')
	for _, header := range [3]string{"Cookie", "Accept", "Turbo-Frame"} {
		values := r.Header.Values(header)
		b = strconv.AppendInt(b, int64(len(values)), 10)
		b = append(b, ':')
		for _, value := range values {
			field(value)
		}
	}
	field(r.UserAgent())
	field(r.Header.Get("Origin"))
	field(r.Header.Get("X-Requested-With"))
	if round.gzip {
		b = append(b, "gzip|"...)
	} else {
		b = append(b, "idn|"...)
	}
	field(os.Getenv("GIT_REVISION"))
	b = strconv.AppendUint(b, version, 10)
	return b
}

// serveCached writes a cache hit: the stored entity headers onto the live
// writer, then the stored wire body through the response buffer's parts
// channel (no copy; finish computes Content-Length from the part and answers
// conditional requests against the stored validator exactly like the map
// path). A framed request skipped the fixed security headers in ServeHTTP;
// they are restored here so the hit head is byte-identical to the fresh
// response's.
func (s *Server) serveCached(entry *cachedResponse, w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	for key, values := range entry.header {
		h[key] = append([]string(nil), values...)
	}
	if buffered := findResponseBuffer(w); buffered != nil {
		if buffered.framed {
			s.restoreRecordedHeaders(h)
		}
		buffered.parts = buffered.partsBuf[:1]
		buffered.parts[0] = entry.body
		return
	}
	// No response buffer in the chain (direct capture writers): write the
	// body through. Headers and status were set above.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(entry.body)
}

// cacheResponse stores the completed response in the whole-response cache.
// It runs in finish before emission, so the store sees exactly the wire body
// and the final header state the request is about to receive — on the framed
// path (where the entity headers live in the precomposed head block) and the
// map path alike. Only completed, non-exceptional, cacheable GET responses
// enter; everything else (flash renders, conditional 304s, failures, zstd
// bodies, non-HTML content) falls through untouched.
func (s *Server) cacheResponse(r *http.Request, w *responseBuffer) {
	info := requestMetadata(r.Context())
	if info == nil || info.response == nil {
		return
	}
	round := info.response
	if round.hit || round.flash || !round.keyed || r.Method != "GET" || w.status != 200 || w.exception {
		return
	}
	if w.wireEncoding != "" && w.wireEncoding != "gzip" || w.wireEncoding == "gzip" != round.gzip {
		// A zstd-coded response is never stored; the stored coding must be
		// the one the key names.
		return
	}
	h := w.Header()
	if h.Get("Content-Type") == "" {
		// The framed path keeps the entity headers out of the map; a framed
		// response is a recorded page, so the constants apply.
		if w.recordedStatus != 0 && w.recordedStatus != http.StatusOK {
			return
		}
	} else {
		if !strings.HasPrefix(h.Get("Content-Type"), "text/html") {
			return
		}
		if control := h.Get("Cache-Control"); strings.Contains(control, "no-store") || strings.Contains(control, "no-transform") {
			return
		}
	}
	wire := w.encoded
	var joined []byte
	if wire == nil && len(w.parts) > 0 {
		total := 0
		for _, part := range w.parts {
			total += len(part)
		}
		joined = make([]byte, 0, total)
		for _, part := range w.parts {
			joined = append(joined, part...)
		}
		wire = joined
	}
	if wire == nil {
		wire = w.body.Bytes()
	}
	// The key is rebuilt into a stack buffer (the same bytes responseHit
	// hashed) so the entry can keep the exact key without a per-request copy;
	// only the admission clones it.
	var keyBuf [1536]byte
	key := appendResponseCacheKey(keyBuf[:0], info, r, round, round.version)
	if len(key) > 8192 || len(wire) == 0 || len(wire)+len(key)+1024 > s.responses.limit/8 {
		return
	}
	header := make(http.Header)
	if h.Get("Content-Type") == "" {
		// Framed: reconstruct the entity headers from the head block's
		// inputs, in the same values the map path emits.
		header.Set("Content-Type", "text/html; charset=utf-8")
		header.Set("Cache-Control", "max-age=0, private, must-revalidate")
		header.Set("Vary", "Accept-Encoding")
		if w.recordedEtag != nil {
			header.Set("ETag", string(append([]byte(nil), w.recordedEtag...)))
		}
		if modified := h.Get("Last-Modified"); modified != "" {
			header.Set("Last-Modified", modified)
		}
		if w.wireEncoding != "" {
			header.Set("Content-Encoding", w.wireEncoding)
		}
	} else {
		for _, name := range []string{"Content-Type", "ETag", "Last-Modified", "Cache-Control", "Link", "Vary", "Content-Encoding"} {
			if values := h.Values(name); len(values) > 0 {
				header[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
			}
		}
	}
	if !strings.Contains(header.Get("Vary"), "Accept-Encoding") {
		header.Add("Vary", "Accept-Encoding")
	}
	entry := &cachedResponse{digest: sha256.Sum256(key), key: bytes.Clone(key), version: round.version, body: bytes.Clone(wire), header: header}
	entry.cost = cap(entry.body) + len(entry.key) + 1024
	for name, values := range header {
		entry.cost += len(name)
		for _, value := range values {
			entry.cost += len(value)
		}
	}
	// The single fresh generation read of the admission window: a commit since
	// the pre-read round must not be admitted under the old generation. The
	// hit path's own fresh read rejects a commit between the round and the hit;
	// this one rejects a commit that landed during the render. The two reads
	// the store used to take (one before the wire assembly, one here) were
	// redundant — both compared against the same pre-read generation — so the
	// store now takes exactly one, as late as possible before the put.
	version, err := s.DB.ResponseVersion(r.Context())
	if err == nil && version == round.version {
		s.responses.put(entry)
	}
}

func responseCacheBudget() (int, error) {
	raw, ok := os.LookupEnv("CAMPFIRE_RESPONSE_CACHE_MB")
	if !ok || raw == "" {
		return 64 << 20, nil
	}
	mb, err := strconv.Atoi(raw)
	if err != nil || mb < 0 || mb > 1024 {
		return 0, strconv.ErrSyntax
	}
	return mb << 20, nil
}
