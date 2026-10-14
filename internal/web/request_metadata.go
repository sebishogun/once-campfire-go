package web

import (
	"context"
	"net/http"
	"sync"

	"github.com/basecamp/once-campfire-go/internal/useragent"
)

type requestInfoKey struct{}

// One request-owned context value replaces separate host/origin values. Parsing
// stays lazy for non-browser endpoints, and derived contexts share the result.
type requestInfo struct {
	host, origin      string
	response          *responseRound
	databaseVersion   uint64
	generationChecked bool
	generationCurrent bool
	agentOnce         sync.Once
	agent             *useragent.Agent
}

func requestMetadata(ctx context.Context) *requestInfo {
	info, _ := ctx.Value(requestInfoKey{}).(*requestInfo)
	return info
}

// contextGeneration reuses the pre-read observation for the entire request.
// Failed observations bypass caching; retrying later could admit earlier reads
// under a newer generation. Non-HTTP callers observe before their own read.
func (s *Server) contextGeneration(ctx context.Context) (uint64, bool) {
	if info := requestMetadata(ctx); info != nil {
		return info.databaseVersion, info.databaseVersion != 0
	}
	version, err := s.DB.ResponseVersion(ctx)
	return version, err == nil && version != 0
}

func requestAgent(r *http.Request) useragent.Agent {
	if info := requestMetadata(r.Context()); info != nil {
		info.agentOnce.Do(func() {
			agent := useragent.Parse(r.UserAgent())
			info.agent = &agent
		})
		return *info.agent
	}
	// Direct rendering and background helpers do not have an HTTP entry context.
	return useragent.Parse(r.UserAgent())
}
