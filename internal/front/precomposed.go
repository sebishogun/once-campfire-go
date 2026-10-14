package front

// The public chain's precomposed seam (the web layer's ENGINE-49 head lane).
//
// The application builds a read page's response head as one sorted byte block
// and hands it to the owned loop in a single call (WritePrecomposed), skipping
// the http.Header map entirely. The front's capture wrapper sits between the
// application and the public response policy on every public request, and it
// used to be deliberately opaque (no Unwrap), which stopped the web layer's
// receiver walk: `framed` was false and every public read response paid the
// map path (13 Set calls, the header sort, the ETag/Vary/Content-Length
// formatting) that the lane exists to remove.
//
// The wrapper is now transparent when — and only when — the chain can actually
// emit a precomposed response, and it stays in the path by being the receiver
// itself: the application's WritePrecomposed call lands on the front wrapper,
// which splices in the header lines the live map carries but the block does
// not (the front's X-Cache marker above all, which the map path emits) and
// forwards to the next receiver. The public compression wrapper is one of
// those receivers, so its own policy (Vary: Accept-Encoding) still reaches the
// emitted head.
//
// Two properties keep this safe:
//
//   - The front cache can never store one of these responses: the application's
//     recorded pages carry "Cache-Control: max-age=0, private, must-revalidate",
//     and lifetime() requires a `public` directive, so the capture is always a
//     pass-through (the head block, not the map, is the emitted head either
//     way). A response the front could store would still be emitted correctly
//     and simply not cached.
//   - A chain that cannot emit a precomposed response (a net/http target
//     listener, the synthetic fixed-route fill over a captureSink) keeps the
//     opaque wrapper, so the application takes the byte-identical map path
//     there. Cache.Handler installs a seam only after finding a receiver on
//     the chain.

import (
	"bytes"
	"net/http"
)

// precomposedReceiver mirrors internal/web's precomposed seam. The front
// cannot import internal/web (web imports front), so the interface is
// restated; Go's structural typing makes the two identical.
type precomposedReceiver interface {
	WritePrecomposed(status int, head []byte, body [][]byte) error
}

// precomposedForwarder is implemented by the front's own policy wrappers: they
// accept a precomposed response but forward it rather than emit it, so a chain
// that ends in one (a net/http target listener behind PublicCompression)
// cannot serve the lane and must keep the opaque capture.
type precomposedForwarder interface {
	forwardsPrecomposed()
}

// canPrecompose reports whether the chain ends in a writer that can emit a
// precomposed response: the owned loop's response. The walk follows Unwrap to
// the end, skipping the front's own forwarding wrappers.
func canPrecompose(w http.ResponseWriter) bool {
	for {
		if _, ok := w.(precomposedReceiver); ok {
			if _, forwards := w.(precomposedForwarder); !forwards {
				return true
			}
		}
		wrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return false
		}
		w = wrapper.Unwrap()
	}
}

// findPrecomposedReceiver walks the writer chain (the same Unwrap walk the web
// layer's lookup uses) for a precomposed receiver.
func findPrecomposedReceiver(w http.ResponseWriter) precomposedReceiver {
	for {
		if receiver, ok := w.(precomposedReceiver); ok {
			return receiver
		}
		wrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return nil
		}
		w = wrapper.Unwrap()
	}
}

// seamWriterFor returns the writer the application sees on the bypass path:
// the writer itself when the chain cannot emit a precomposed response, or a
// seam that keeps the front's X-Cache marker on the wire when it can.
func seamWriterFor(w http.ResponseWriter) http.ResponseWriter {
	if canPrecompose(w) {
		return &seamWriter{w}
	}
	return w
}

// captureWriterFor returns the writer the application sees on the miss path:
// the capture itself when the chain cannot emit a precomposed response (the
// application then takes the byte-identical map path), or the capture's own
// embedded seam when it can. The seam is a field of the capture, so the
// transparent writer adds no allocation to the request.
func captureWriterFor(capture *recordResponse) http.ResponseWriter {
	if !canPrecompose(capture.ResponseWriter) {
		return capture
	}
	capture.seam = captureWriter{capture}
	return &capture.seam
}

// captureWriter is the capture wrapper the public miss path installs when the
// chain can emit a precomposed response: the ordinary recordResponse
// bookkeeping plus the precomposed seam. It is the only front wrapper that
// unwraps, so the web layer's receiver walk reaches the loop through it while
// the application's precomposed write still lands on the front.
type captureWriter struct {
	*recordResponse
}

func (w *captureWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *captureWriter) forwardsPrecomposed()        {}

// WritePrecomposed emits the application's precomposed response through the
// front. The wrapper takes the status for its bookkeeping (so the deferred
// write paths cannot emit a second head) and forwards through the seam.
//
// Nothing is captured: the application's precomposed responses are its own
// cached pages, which are private and so never accepted by lifetime().
func (w *captureWriter) WritePrecomposed(status int, head []byte, body [][]byte) error {
	w.status = status
	return forwardPrecomposed(w.ResponseWriter, status, head, body)
}

// seamWriter is the bypass path's writer: the response is not cacheable by
// construction (the cache round did not run), so the seam carries no capture —
// only the precomposed forwarding, which keeps the front's X-Cache marker on
// the wire when the application frames the response.
type seamWriter struct {
	http.ResponseWriter
}

func (w *seamWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *seamWriter) forwardsPrecomposed()        {}
func (w *seamWriter) WritePrecomposed(status int, head []byte, body [][]byte) error {
	return forwardPrecomposed(w.ResponseWriter, status, head, body)
}

// forwardPrecomposed hands the application's head block and body to the next
// precomposed receiver in the chain, with the header lines the live map
// carries but the block does not spliced in. The receiver's own WriteHeader
// policy never ran on this path, so a policy that must reach the emitted head
// (the compression wrapper's Vary) implements WritePrecomposed itself and
// forwards in turn.
func forwardPrecomposed(next http.ResponseWriter, status int, head []byte, body [][]byte) error {
	receiver := findPrecomposedReceiver(next)
	if receiver == nil {
		// Unreachable: a seam is installed only after finding a receiver on
		// the chain, and nothing rewrites the chain in between. There is no
		// map to fall back to (the framed path never built one).
		return nil
	}
	return receiver.WritePrecomposed(status, spliceMissingHeaders(head, next.Header(), status), body)
}

// spliceMissingHeaders completes the application's head block with the header
// lines the map path would emit on top of it.
//
// The application's 200 block is rendered from the same values the map path
// emits — the header block "in the map path's exact order", as its own
// documentation puts it — so the front's X-Cache marker, which the block never
// carries, is the whole job; the map walk that guarantees the rest is skipped
// on that path because it costs ~370 ns, a fifth of the lane's saving. Any
// other status is a suppression of the 200 block (the 304 drops the entity
// headers, Content-Encoding among them), so those blocks are completed from
// the map, which is the shape the map path's own 304 has.
func spliceMissingHeaders(head []byte, h http.Header, status int) []byte {
	if marker := h.Get("X-Cache"); marker != "" {
		head = insertHeadLine(head, "X-Cache", marker)
	}
	if status != http.StatusOK {
		for name, values := range h {
			for _, value := range values {
				if value == "" {
					continue
				}
				head = insertHeadLine(head, name, value)
			}
		}
	}
	return head
}

// insertHeadLine inserts one rendered "Name: value\r\n" line into a head block
// sorted by canonical header name, keeping that order. The line is built in a
// stack buffer, so the call allocates nothing when the block has spare
// capacity. Presence is tested at a line start, so a value that happens to
// contain the name cannot suppress the insert.
func insertHeadLine(head []byte, name, value string) []byte {
	var lineBuf, needleBuf [80]byte
	line := append(lineBuf[:0], name...)
	line = append(line, ':', ' ')
	nameEnd := len(line) // "Name: "
	line = append(line, value...)
	line = append(line, '\r', '\n')
	needle := append(needleBuf[:0], '\r', '\n')
	needle = append(needle, line[:nameEnd]...)
	if bytes.HasPrefix(head, line[:nameEnd]) || bytes.Contains(head, needle) {
		return head
	}
	at := len(head)
	for i := 0; i < len(head); {
		end := bytes.IndexByte(head[i:], '\n')
		if end < 0 {
			break
		}
		end += i + 1
		if colon := bytes.IndexByte(head[i:end], ':'); colon >= 0 && bytes.Compare(head[i:i+colon], line[:len(name)]) > 0 {
			at = i
			break
		}
		i = end
	}
	if cap(head)-len(head) >= len(line) {
		head = head[:len(head)+len(line)]
		copy(head[at+len(line):], head[at:])
		copy(head[at:], line)
		return head
	}
	grown := make([]byte, 0, len(head)+len(line))
	grown = append(grown, head[:at]...)
	grown = append(grown, line...)
	return append(grown, head[at:]...)
}
