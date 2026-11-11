package simulator

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"nexuslb/pkg/backend"
	"nexuslb/pkg/balancer"
	"nexuslb/pkg/metrics"
	"nexuslb/pkg/proxy"
	"nexuslb/pkg/ratelimit"
)

func setupRealProxyEnv(t *testing.T, rate float64, burst int) (
	proxySrv *httptest.Server,
	backends []*backend.Backend,
	sm *balancer.StrategyManager,
	collector *metrics.Collector,
	limiter *ratelimit.Limiter,
	cleanup func(),
) {
	t.Helper()

	var upstreams []*httptest.Server
	for i := 0; i < 3; i++ {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "ok")
		}))
		upstreams = append(upstreams, srv)
		b, err := backend.NewBackend(srv.URL)
		if err != nil {
			t.Fatalf("backend create failed: %v", err)
		}
		backends = append(backends, b)
	}

	var err error
	sm, err = balancer.NewStrategyManager("adaptive", backends)
	if err != nil {
		t.Fatalf("strategy manager init failed: %v", err)
	}

	collector = metrics.NewCollector()
	limiter = ratelimit.NewLimiter(rate, burst, true)
	handler := proxy.NewProxyHandler(sm, collector, limiter)
	proxySrv = httptest.NewServer(handler)

	cleanup = func() {
		proxySrv.Close()
		limiter.Close()
		for _, u := range upstreams {
			u.Close()
		}
	}
	return proxySrv, backends, sm, collector, limiter, cleanup
}

func TestSimulator_OrganicMixedTrafficAndIPDiversity(t *testing.T) {
	proxySrv, backends, sm, collector, _, cleanup := setupRealProxyEnv(t, 100.0, 200)
	defer cleanup()

	var seenIPs sync.Map
	// Wrap proxy handler check via metrics logs after run
	sim := New(SimConfig{
		Intensity:  IntensityHeavy,
		AllowChaos: false,
		AllowDDoS:  false,
	}, ClusterHooks{
		ProxyURL:        proxySrv.URL,
		GetBackends:     func() []*backend.Backend { return backends },
		StrategyKey:     sm.CurrentKey,
		TickInterval:    25 * time.Millisecond,
		BaselineHoldMin: 5 * time.Second,
		BaselineHoldMax: 6 * time.Second,
	})

	sim.Start()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := sim.Snapshot()
		if snap.Counters.TotalSent >= 30 && snap.Counters.UniqueIPReqs > 0 && snap.Counters.RepeatIPReqs > 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	sim.Stop()

	snap := sim.Snapshot()
	if snap.Running {
		t.Fatal("expected simulator to report stopped after Stop()")
	}
	if snap.Counters.TotalSent < 20 {
		t.Fatalf("expected >= 20 simulated requests dispatched, got %d", snap.Counters.TotalSent)
	}
	if snap.Counters.UniqueIPReqs == 0 || snap.Counters.RepeatIPReqs == 0 {
		t.Errorf("expected both unique IPs and repeat IPs in organic mix; got unique=%d repeat=%d",
			snap.Counters.UniqueIPReqs, snap.Counters.RepeatIPReqs)
	}

	stats := collector.Snapshot(sm.Name(), backends)
	if stats.TotalSuccess == 0 {
		t.Errorf("expected upstream successes recorded in collector, got %+v", stats)
	}
	for _, entry := range stats.RecentLogs {
		if entry.ClientIP != "" {
			seenIPs.Store(entry.ClientIP, true)
		}
	}
	ipCount := 0
	seenIPs.Range(func(_, _ interface{}) bool {
		ipCount++
		return true
	})
	if ipCount < 3 {
		t.Errorf("expected multiple distinct client IPs in recent logs, got %d", ipCount)
	}
}

func TestSimulator_DDoSWavesAndEdgeShield429(t *testing.T) {
	// Strict per-IP token bucket so DDoS triggers immediate 429 blocks
	proxySrv, backends, sm, collector, _, cleanup := setupRealProxyEnv(t, 2.0, 2)
	defer cleanup()

	sim := New(SimConfig{
		Intensity:  IntensityNormal,
		AllowChaos: false,
		AllowDDoS:  true,
	}, ClusterHooks{
		ProxyURL:        proxySrv.URL,
		GetBackends:     func() []*backend.Backend { return backends },
		TickInterval:    20 * time.Millisecond,
		BaselineHoldMin: 5 * time.Second,
		BaselineHoldMax: 6 * time.Second,
		WaveDurationMin: 250 * time.Millisecond,
		WaveDurationMax: 350 * time.Millisecond,
	})
	defer sim.Stop()

	if err := sim.TriggerWave(WaveSingleDoS); err != nil {
		t.Fatalf("TriggerWave SingleDoS failed: %v", err)
	}

	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		snap := sim.Snapshot()
		if snap.Counters.AttackReqs > 0 && snap.Counters.Blocked429 > 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Preempt with distributed botnet DDoS wave
	if err := sim.TriggerWave(WaveBotnetDDoS); err != nil {
		t.Fatalf("TriggerWave BotnetDDoS failed: %v", err)
	}

	sawBotnet := false
	deadline2 := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline2) {
		snap := sim.Snapshot()
		if snap.ActiveWave == WaveBotnetDDoS && snap.ActiveAttackerIPs >= 5 {
			sawBotnet = true
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !sawBotnet {
		t.Errorf("expected botnet DDoS wave with >= 5 attacker IPs, snapshot=%+v", sim.Snapshot())
	}

	time.Sleep(150 * time.Millisecond)
	sim.Stop()

	snap := sim.Snapshot()
	if snap.Counters.AttackReqs == 0 || snap.Counters.Blocked429 == 0 {
		t.Errorf("expected attack traffic and Edge Shield 429 responses; got attack=%d blocked=%d",
			snap.Counters.AttackReqs, snap.Counters.Blocked429)
	}

	stats := collector.Snapshot(sm.Name(), backends)
	if stats.TotalThrottled == 0 {
		t.Errorf("expected collector TotalThrottled > 0 under DDoS waves, got 0")
	}
}

func TestSimulator_ReversibleChaosAndStopRestore(t *testing.T) {
	proxySrv, backends, _, _, _, cleanup := setupRealProxyEnv(t, 100.0, 200)
	defer cleanup()

	// Configure non-trivial initial baseline so restoration proves exact baseline preservation
	backends[0].SetWeight(2)
	backends[1].SetWeight(3)
	backends[2].SetWeight(1)

	var delayMux sync.Mutex
	delayCalls := make(map[string]int64)
	var healthChecks int64
	var downCalls int64

	sim := New(SimConfig{
		Intensity:  IntensityNormal,
		AllowChaos: true,
		AllowDDoS:  true,
	}, ClusterHooks{
		ProxyURL:    proxySrv.URL,
		GetBackends: func() []*backend.Backend { return backends },
		SetBackendDelay: func(u string, ms int64) error {
			delayMux.Lock()
			delayCalls[u] = ms
			delayMux.Unlock()
			for _, b := range backends {
				if b.URL.String() == u {
					b.SetSimulatedDelay(ms)
				}
			}
			return nil
		},
		ToggleBackendHealth: func(u, state string) error {
			if strings.EqualFold(state, "down") {
				atomic.AddInt64(&downCalls, 1)
			}
			for _, b := range backends {
				if b.URL.String() == u {
					b.SetAlive(state != "down")
				}
			}
			return nil
		},
		CheckHealth:     func() { atomic.AddInt64(&healthChecks, 1) },
		TickInterval:    25 * time.Millisecond,
		BaselineHoldMin: 5 * time.Second,
		BaselineHoldMax: 6 * time.Second,
		WaveDurationMin: 400 * time.Millisecond,
		WaveDurationMax: 500 * time.Millisecond,
	})

	// 1. Latency spike wave + immediate Stop mid-wave must revert simulated delay back to 0
	if err := sim.TriggerWave(WaveLatency); err != nil {
		t.Fatalf("TriggerWave Latency failed: %v", err)
	}
	deadline := time.Now().Add(1 * time.Second)
	sawDelay := false
	for time.Now().Before(deadline) {
		for _, b := range backends {
			if b.GetSimulatedDelay() > 0 {
				sawDelay = true
				break
			}
		}
		if sawDelay {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !sawDelay {
		t.Fatal("expected latency spike wave to inject non-zero delay on a backend")
	}
	sim.Stop()

	for _, b := range backends {
		if b.GetSimulatedDelay() != 0 {
			t.Errorf("expected Stop() to restore %s delay to 0, got %d", b.URL.String(), b.GetSimulatedDelay())
		}
	}

	// 2. Canary weighted rollout wave must alter then restore weights & canary flags cleanly
	if err := sim.TriggerWave(WaveCanaryWRR); err != nil {
		t.Fatalf("TriggerWave CanaryWRR failed: %v", err)
	}
	deadlineCanary := time.Now().Add(1 * time.Second)
	sawCanary := false
	for time.Now().Before(deadlineCanary) {
		for _, b := range backends {
			if b.IsCanaryNode() {
				sawCanary = true
				break
			}
		}
		if sawCanary {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if !sawCanary {
		t.Fatal("expected Canary+WRR wave to mark a canary node")
	}
	sim.Stop()

	expectedWeights := []int64{2, 3, 1}
	for i, b := range backends {
		if b.GetWeight() != expectedWeights[i] {
			t.Errorf("backend %d weight not restored: want %d, got %d", i, expectedWeights[i], b.GetWeight())
		}
		if b.IsCanaryNode() {
			t.Errorf("backend %d canary flag not restored to false", i)
		}
	}

	// 3. Node outage wave + recovery check
	if err := sim.TriggerWave(WaveNodeOutage); err != nil {
		t.Fatalf("TriggerWave NodeOutage failed: %v", err)
	}
	deadlineOutage := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadlineOutage) && atomic.LoadInt64(&downCalls) == 0 {
		time.Sleep(15 * time.Millisecond)
	}
	if atomic.LoadInt64(&downCalls) == 0 {
		t.Fatal("expected node outage wave to invoke ToggleBackendHealth down")
	}
	sim.Stop()

	for _, b := range backends {
		if !b.IsAlive() {
			t.Errorf("expected Stop() to restore backend %s back healthy", b.URL.String())
		}
	}
	if atomic.LoadInt64(&healthChecks) == 0 {
		t.Errorf("expected CheckHealth to be invoked on outage recovery")
	}
	_ = delayCalls
}

func TestSimulator_ConfigureAndReset(t *testing.T) {
	sim := New(SimConfig{
		Intensity:  IntensityLow,
		AllowChaos: true,
		AllowDDoS:  true,
	}, ClusterHooks{ProxyURL: "http://127.0.0.1:65535"})

	if _, ok := ParseWave("invalid_wave"); ok {
		t.Error("expected ParseWave invalid_wave to fail")
	}
	if err := sim.TriggerWave(WaveKind("invalid")); err == nil {
		t.Error("expected TriggerWave invalid to return error")
	}

	heavy := IntensityHeavy
	f := false
	sim.Configure(&heavy, &f, &f)
	snap := sim.Snapshot()
	if snap.Intensity != IntensityHeavy || snap.AllowChaos || snap.AllowDDoS {
		t.Errorf("unexpected configured state: %+v", snap)
	}

	sim.recordEvent(WaveBaseline, "Test", "Detail", "info")
	atomic.AddInt64(&sim.totalSent, 12)
	sim.Reset()

	after := sim.Snapshot()
	if after.Counters.TotalSent != 0 || len(after.Events) != 0 {
		t.Errorf("expected Reset() to clear counters and events, got %+v", after)
	}
}

func TestSimulator_DefaultChaosHooksNilSafe(t *testing.T) {
	var downToggleCount int64
	var delaySetCount int64

	newChaosBackend := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/chaos/toggle":
				if r.URL.Query().Get("state") == "down" {
					atomic.AddInt64(&downToggleCount, 1)
				}
				w.WriteHeader(http.StatusOK)
			case "/chaos/delay":
				atomic.AddInt64(&delaySetCount, 1)
				w.WriteHeader(http.StatusOK)
			default:
				w.WriteHeader(http.StatusOK)
			}
		}))
	}

	s1 := newChaosBackend()
	defer s1.Close()
	s2 := newChaosBackend()
	defer s2.Close()

	b1, err := backend.NewBackend(s1.URL)
	if err != nil {
		t.Fatalf("NewBackend b1 failed: %v", err)
	}
	b2, err := backend.NewBackend(s2.URL)
	if err != nil {
		t.Fatalf("NewBackend b2 failed: %v", err)
	}
	backends := []*backend.Backend{b1, b2}

	sm, _ := balancer.NewStrategyManager("round_robin", backends)
	px := proxy.NewProxyHandler(sm, metrics.NewCollector(), ratelimit.NewLimiter(100, 100, false))
	pxSrv := httptest.NewServer(px)
	defer pxSrv.Close()

	// Intentionally leave SetBackendDelay and ToggleBackendHealth nil to verify default wiring
	sim := New(SimConfig{
		Intensity:  IntensityNormal,
		AllowChaos: true,
		AllowDDoS:  true,
	}, ClusterHooks{
		ProxyURL:        pxSrv.URL,
		GetBackends:     sm.GetBackends,
		TickInterval:    25 * time.Millisecond,
		BaselineHoldMin: 5 * time.Second,
		BaselineHoldMax: 6 * time.Second,
		WaveDurationMin: 300 * time.Millisecond,
		WaveDurationMax: 400 * time.Millisecond,
	})

	if sim.hooks.SetBackendDelay == nil || sim.hooks.ToggleBackendHealth == nil {
		t.Fatal("expected New to populate default SetBackendDelay and ToggleBackendHealth hooks")
	}

	if err := sim.TriggerWave(WaveLatency); err != nil {
		t.Fatalf("TriggerWave Latency failed: %v", err)
	}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt64(&delaySetCount) == 0 {
		time.Sleep(15 * time.Millisecond)
	}
	if atomic.LoadInt64(&delaySetCount) == 0 {
		t.Fatal("expected default SetBackendDelay hook to call /chaos/delay")
	}
	sim.Stop()

	if err := sim.TriggerWave(WaveNodeOutage); err != nil {
		t.Fatalf("TriggerWave NodeOutage failed: %v", err)
	}
	deadline = time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt64(&downToggleCount) == 0 {
		time.Sleep(15 * time.Millisecond)
	}
	if atomic.LoadInt64(&downToggleCount) == 0 {
		t.Fatal("expected default ToggleBackendHealth hook to call /chaos/toggle")
	}
	sim.Stop()
}

