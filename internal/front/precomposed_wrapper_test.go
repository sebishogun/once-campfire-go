package front

// Tests for the front's precomposed surfaces on the bypass path
// (precomposed.go): the seam the application sees when the front can complete a
// precomposed response, and the blocking surface it sees when it cannot. The
// policy wrapper advertises a seam it can only forward, so a chain with no
// emitter behind it must never be left to reach the wrapper directly.

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestPrecomposedBypassSurfaceHidesTheSeam pins the mechanism the bypass
// regression depends on: the blocking surface stops the receiver walk, the
// seam continues it to the emitter, and the policy wrapper on its own — which
// advertises a seam it cannot complete without an emitter behind it — is never
// what the application sees.
func TestPrecomposedBypassSurfaceHidesTheSeam(t *testing.T) {
	request := httptest.NewRequest("GET", "/rooms/1", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	emitting := &publicResponse{ResponseWriter: &emitterWriter{ResponseWriter: &shimWriter{header: http.Header{}}}, request: request, selected: "gzip"}
	if got := findPrecomposedReceiver(bypassWriter(emitting, request)); got == nil {
		t.Fatal("the bypass seam did not expose a receiver to the emitter")
	}
	sealed := &publicResponse{ResponseWriter: &shimWriter{header: http.Header{}}, request: request, selected: "gzip"}
	if got := findPrecomposedReceiver(bypassWriter(sealed, request)); got != nil {
		t.Fatalf("the blocking surface exposed %T with no emitter downstream", got)
	}
	// The policy wrapper itself does advertise the seam: that is exactly why
	// the blocking surface exists.
	if got := findPrecomposedReceiver(&publicResponse{ResponseWriter: &shimWriter{header: http.Header{}}, request: request, selected: "gzip"}); got == nil {
		t.Fatal("the policy wrapper no longer advertises the seam, so this regression cannot recur")
	}
}

// TestPrecomposedBypassWrapperAllocatesNothing pins the bypass path's wrapper
// allocation: both surfaces are fields of the request's policy wrapper, so
// installing either costs nothing. A chain without a policy wrapper
// (compression disabled) has no request-owned home; that one cold-path
// allocation is pinned too, so it cannot spread.
func TestPrecomposedBypassWrapperAllocatesNothing(t *testing.T) {
	request := httptest.NewRequest("GET", "/rooms/1", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	var got http.ResponseWriter

	emitting := &publicResponse{ResponseWriter: &emitterWriter{ResponseWriter: &shimWriter{header: http.Header{}}}, request: request, selected: "gzip"}
	if allocs := testing.AllocsPerRun(200, func() { got = bypassWriter(emitting, request) }); allocs != 0 {
		t.Fatalf("seam install allocated %v objects, want 0", allocs)
	}
	if got != &emitting.seam {
		t.Fatalf("seam install returned %T, want the policy wrapper's own seam", got)
	}

	sealed := &publicResponse{ResponseWriter: &shimWriter{header: http.Header{}}, request: request, selected: "gzip"}
	if allocs := testing.AllocsPerRun(200, func() { got = bypassWriter(sealed, request) }); allocs != 0 {
		t.Fatalf("blocking install allocated %v objects, want 0", allocs)
	}
	if got != &sealed.block {
		t.Fatalf("blocking install returned %T, want the policy wrapper's own surface", got)
	}

	bare := &emitterWriter{ResponseWriter: &shimWriter{header: http.Header{}}}
	if allocs := testing.AllocsPerRun(200, func() { got = bypassWriter(bare, request) }); allocs != 1 {
		t.Fatalf("no-policy seam install allocated %v objects, want the documented 1", allocs)
	}
}
