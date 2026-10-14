package web

import (
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/basecamp/once-campfire-go/internal/database"
)

// BenchmarkCacheKeys measures the per-request key builders the whole-response
// and sidebar-page caches run on every lookup. Both must allocate nothing: the
// key is built into a stack buffer, hashed in place into the [32]byte map key,
// and only an admission copies it into the entry. Before this change the
// response key returned a string (2 allocs / 152 B per call on this shape) and
// the sidebar key went through fmt.Sprintf.
func BenchmarkCacheKeys(b *testing.B) {
	b.Run("response", func(b *testing.B) {
		info := &requestInfo{host: "campfire.test", origin: "http://campfire.test"}
		r := httptest.NewRequest("GET", "/searches?q=parrot", nil)
		r.Form = map[string][]string{"q": {"parrot"}}
		round := &responseRound{version: 42, user: 7, gzip: true}
		var sink [32]byte
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var buf [1536]byte
			sink = sha256.Sum256(appendResponseCacheKey(buf[:0], info, r, round, 42))
		}
		_ = sink
	})
	b.Run("sidebar", func(b *testing.B) {
		u := database.User{ID: 3, Name: "fixture", Role: 1}
		var sink [32]byte
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var buf [256]byte
			sink = sha256.Sum256(appendSidebarPageKey(buf[:0], 7, 1, u, false))
		}
		_ = sink
	})
}

// BenchmarkWarmSearchHandler measures the app handler cost of a warm search
// served by the whole-response cache against the same route served only by the
// search result cache (CAMPFIRE_RESPONSE_CACHE_MB=0 disables response storage,
// so the round never starts and the result cache serves). It is the shape
// Strategy 2 (searches#index in the response round) is judged on.
func BenchmarkWarmSearchHandler(b *testing.B) {
	run := func(b *testing.B, responseMB string) {
		if responseMB != "" {
			b.Setenv("CAMPFIRE_RESPONSE_CACHE_MB", responseMB)
		}
		app, _, cookie := benchApp(b)
		makeReq := func() *http.Request {
			req := httptest.NewRequest("GET", "/searches?q=warm", nil)
			req.Header.Set("Cookie", cookie)
			req.Header.Set("Accept-Encoding", "gzip")
			req.Host = "campfire.test"
			return req
		}
		app.ServeHTTP(httptest.NewRecorder(), makeReq())
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			app.ServeHTTP(httptest.NewRecorder(), makeReq())
		}
	}
	b.Run("response-cache", func(b *testing.B) { run(b, "") })
	b.Run("search-cache-only", func(b *testing.B) { run(b, "0") })
}
