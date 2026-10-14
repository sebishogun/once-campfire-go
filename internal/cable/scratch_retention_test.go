package cable

import "testing"

func TestPublishScratchClearsCompactedTail(t *testing.T) {
	s := &publishScratch{recips: make([]recipient, 4), tokens: make([]string, 4)}
	s.recips[3].client = &client{}
	s.tokens[3] = "retained session"
	s.recips = s.recips[:1]
	s.tokens = s.tokens[:1]
	s.reset()
	for _, r := range s.recips[:cap(s.recips)] {
		if r.client != nil {
			t.Fatal("scratch retains client in compacted tail")
		}
	}
	for _, token := range s.tokens[:cap(s.tokens)] {
		if token != "" {
			t.Fatal("scratch retains token in compacted tail")
		}
	}
}
