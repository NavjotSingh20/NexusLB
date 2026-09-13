package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"nexuslb/pkg/backend"
	"nexuslb/pkg/balancer"
	"nexuslb/pkg/metrics"
)

func setupTestServer(t *testing.T) (*Server, *balancer.StrategyManager) {
	b1, err := backend.NewBackend("http://localhost:8001")
	if err != nil {
		t.Fatal(err)
	}
	b2, err := backend.NewBackend("http://localhost:8002")
	if err != nil {
		t.Fatal(err)
	}
	backends := []*backend.Backend{b1, b2}

	sm, err := balancer.NewStrategyManager("round_robin", backends)
	if err != nil {
		t.Fatal(err)
	}

	collector := metrics.NewCollector()
	srv := NewServer(":8081", ":8080", collector, sm, nil)
	return srv, sm
}

func TestServer_StrategyAPI(t *testing.T) {
	srv, sm := setupTestServer(t)

	// 1. Test GET /api/strategy
	getReq := httptest.NewRequest(http.MethodGet, "/api/strategy", nil)
	getRec := httptest.NewRecorder()
	srv.handleStrategy(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", getRec.Code)
	}

	var getResp struct {
		Strategy  string   `json:"strategy"`
		Key       string   `json:"key"`
		Available []string `json:"available"`
	}
	if err := json.NewDecoder(getRec.Body).Decode(&getResp); err != nil {
		t.Fatalf("failed to decode GET /api/strategy: %v", err)
	}
	if getResp.Key != "round_robin" {
		t.Errorf("expected initial key round_robin, got %s", getResp.Key)
	}
	if len(getResp.Available) != 3 {
		t.Errorf("expected 3 available strategies, got %d", len(getResp.Available))
	}

	// 2. Test POST /api/strategy to switch to least_connections
	postReq := httptest.NewRequest(http.MethodPost, "/api/strategy?name=least_connections", nil)
	postRec := httptest.NewRecorder()
	srv.handleStrategy(postRec, postReq)

	if postRec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", postRec.Code)
	}
	if sm.CurrentKey() != "least_connections" {
		t.Errorf("expected strategy manager to be least_connections, got %s", sm.CurrentKey())
	}

	// 3. Test POST /api/strategy to switch to ip_hash
	postReq2 := httptest.NewRequest(http.MethodPost, "/api/strategy?name=ip_hash", nil)
	postRec2 := httptest.NewRecorder()
	srv.handleStrategy(postRec2, postReq2)

	if postRec2.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", postRec2.Code)
	}
	if sm.CurrentKey() != "ip_hash" {
		t.Errorf("expected strategy manager to be ip_hash, got %s", sm.CurrentKey())
	}

	// 4. Test POST with invalid strategy
	postReqInv := httptest.NewRequest(http.MethodPost, "/api/strategy?name=random_invalid", nil)
	postRecInv := httptest.NewRecorder()
	srv.handleStrategy(postRecInv, postReqInv)

	if postRecInv.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 for invalid strategy, got %d", postRecInv.Code)
	}
}

func TestServer_StatsAPI(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rec := httptest.NewRecorder()
	srv.handleStats(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	var stats metrics.DashboardStats
	if err := json.NewDecoder(rec.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode stats: %v", err)
	}
	if stats.StrategyKey != "round_robin" {
		t.Errorf("expected strategy key round_robin, got %s", stats.StrategyKey)
	}
}

func TestServer_ResetAPI(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/api/stats/reset", nil)
	rec := httptest.NewRecorder()
	srv.handleResetStats(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}

func TestServer_ConnectionsAPI(t *testing.T) {
	srv, _ := setupTestServer(t)

	// Set count to 8
	req := httptest.NewRequest(http.MethodPost, "/api/backend/connections?url=http://localhost:8001&count=8", nil)
	rec := httptest.NewRecorder()
	srv.handleBackendConnections(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// Add delta +2
	req2 := httptest.NewRequest(http.MethodPost, "/api/backend/connections?url=http://localhost:8001&delta=2", nil)
	rec2 := httptest.NewRecorder()
	srv.handleBackendConnections(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec2.Code)
	}

	backends := srv.balancer.GetBackends()
	if backends[0].GetStats().ActiveConnections != 10 {
		t.Errorf("expected 10 active connections, got %d", backends[0].GetStats().ActiveConnections)
	}
}
