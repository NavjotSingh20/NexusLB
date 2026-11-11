package simulator

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"nexuslb/pkg/backend"
)

// Intensity controls simulated request throughput and wave cadence.
type Intensity string

const (
	IntensityLow    Intensity = "low"
	IntensityNormal Intensity = "normal"
	IntensityHeavy  Intensity = "heavy"
)

// WaveKind identifies the current real-life simulation workload or fault scenario.
type WaveKind string

const (
	WaveIdle       WaveKind = "idle"
	WaveBaseline   WaveKind = "organic_baseline"
	WaveSticky     WaveKind = "sticky_affinity"
	WaveSingleDoS  WaveKind = "single_ip_dos"
	WaveBotnetDDoS WaveKind = "botnet_ddos"
	WaveLatency    WaveKind = "latency_spike"
	WaveCanaryWRR  WaveKind = "canary_weighted_rollout"
	WaveNodeOutage WaveKind = "node_outage_failover"
)

// SimConfig configures the real-life autonomous traffic simulator.
type SimConfig struct {
	Enabled    bool      `json:"enabled"`
	Intensity  Intensity `json:"intensity"`
	AllowChaos bool      `json:"allow_chaos"`
	AllowDDoS  bool      `json:"allow_ddos"`
}

// SimEvent describes a simulated traffic wave or reversible chaos transition.
type SimEvent struct {
	Timestamp string   `json:"timestamp"`
	Wave      WaveKind `json:"wave"`
	Title     string   `json:"title"`
	Detail    string   `json:"detail"`
	Severity  string   `json:"severity"` // "info", "ok", "warn", "bad"
}

// SimCounters tracks observed dispatched and mitigated simulation traffic.
type SimCounters struct {
	TotalSent    int64 `json:"total_sent"`
	UniqueIPReqs int64 `json:"unique_ip_reqs"`
	RepeatIPReqs int64 `json:"repeat_ip_reqs"`
	CanaryReqs   int64 `json:"canary_reqs"`
	SlowReqs     int64 `json:"slow_reqs"`
	AttackReqs   int64 `json:"attack_reqs"`
	Blocked429   int64 `json:"blocked_429"`
	ChaosEvents  int64 `json:"chaos_events"`
}

// Snapshot is serialized into DashboardStats for SSE/UI telemetry.
type Snapshot struct {
	Running           bool        `json:"running"`
	Intensity         Intensity   `json:"intensity"`
	AllowChaos        bool        `json:"allow_chaos"`
	AllowDDoS         bool        `json:"allow_ddos"`
	ActiveWave        WaveKind    `json:"active_wave"`
	WaveLabel         string      `json:"wave_label"`
	WaveDetail        string      `json:"wave_detail"`
	TargetRPS         float64     `json:"target_rps"`
	ActiveAttackerIPs int         `json:"active_attacker_ips"`
	Counters          SimCounters `json:"counters"`
	Events            []SimEvent  `json:"events"`
}

// ClusterHooks connects the simulator to the live proxy and cluster controls.
type ClusterHooks struct {
	ProxyURL            string
	GetBackends         func() []*backend.Backend
	SetBackendDelay     func(url string, ms int64) error
	ToggleBackendHealth func(url, state string) error
	CheckHealth         func()
	StrategyKey         func() string
	TickInterval        time.Duration // optional override for tests
	BaselineHoldMin     time.Duration // optional override for tests
	BaselineHoldMax     time.Duration // optional override for tests
	WaveDurationMin     time.Duration // optional override for tests
	WaveDurationMax     time.Duration // optional override for tests
}

type chaosBackup struct {
	delays    map[string]int64
	weights   map[string]int64
	canary    map[string]bool
	canaryTag map[string]string
	downURLs  map[string]bool
}

func newChaosBackup() *chaosBackup {
	return &chaosBackup{
		delays:    make(map[string]int64),
		weights:   make(map[string]int64),
		canary:    make(map[string]bool),
		canaryTag: make(map[string]string),
		downURLs:  make(map[string]bool),
	}
}

const (
	maxSimEvents  = 24
	maxInFlight   = 64
	defaultTickMs = 200 * time.Millisecond
)

var (
	stickyClientIPs = []string{
		"198.51.100.24",
		"198.51.100.77",
		"192.0.2.45",
		"192.0.2.118",
	}
	publicPrefixes = [][2]int{
		{72, 14},
		{104, 28},
		{172, 58},
		{98, 120},
		{49, 36},
		{138, 68},
		{199, 16},
		{64, 233},
	}
	organicRoutes = []struct {
		method string
		path   string
		idem   bool
	}{
		{http.MethodGet, "/api/catalog", false},
		{http.MethodGet, "/api/orders", false},
		{http.MethodGet, "/api/profile", false},
		{http.MethodGet, "/assets/app.js", false},
		{http.MethodGet, "/api/search", false},
		{http.MethodPost, "/api/checkout", true},
		{http.MethodPost, "/api/events", false},
		{http.MethodHead, "/api/healthz", false},
	}
)

// Simulator generates realistic multi-IP, sticky-client, canary, slow-query, DDoS,
// and reversible cluster chaos workloads against the live proxy ingress.
type Simulator struct {
	hooks      ClusterHooks
	httpClient *http.Client

	mu          sync.RWMutex
	running     bool
	intensity   Intensity
	allowChaos  bool
	allowDDoS   bool
	activeWave  WaveKind
	waveLabel   string
	waveDetail  string
	targetRPS   float64
	attackerIPs []string
	pinnedIP    string
	events      []SimEvent

	cancel context.CancelFunc
	wg     sync.WaitGroup

	triggerMux     sync.Mutex
	pendingTrigger WaveKind
	wakeupCh       chan struct{}

	chaosMux            sync.Mutex
	backup              *chaosBackup
	touchedCircuitURLs  map[string]bool

	rngMux sync.Mutex
	rng    *rand.Rand

	sem chan struct{}

	totalSent    int64
	uniqueIPReqs int64
	repeatIPReqs int64
	canaryReqs   int64
	slowReqs     int64
	attackReqs   int64
	blocked429   int64
	chaosEvents  int64
}

// ParseIntensity normalizes an intensity label to Low, Normal, or Heavy.
func ParseIntensity(raw string) (Intensity, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "normal", "medium", "default":
		return IntensityNormal, true
	case "low", "light", "quiet":
		return IntensityLow, true
	case "high", "heavy", "burst", "max":
		return IntensityHeavy, true
	default:
		return IntensityNormal, false
	}
}

// ParseWave parses a wave name requested via API or triggers.
func ParseWave(raw string) (WaveKind, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "organic_baseline", "baseline", "organic":
		return WaveBaseline, true
	case "sticky_affinity", "sticky", "ip_hash", "repeat":
		return WaveSticky, true
	case "single_ip_dos", "single_dos", "dos":
		return WaveSingleDoS, true
	case "botnet_ddos", "botnet", "ddos":
		return WaveBotnetDDoS, true
	case "latency_spike", "latency", "lag", "least_connections":
		return WaveLatency, true
	case "canary_weighted_rollout", "canary", "weighted", "wrr":
		return WaveCanaryWRR, true
	case "node_outage_failover", "outage", "failover", "circuit_breaker":
		return WaveNodeOutage, true
	default:
		return "", false
	}
}

// New constructs a Simulator with safe defaults and HTTP hook fallbacks.
func New(cfg SimConfig, hooks ClusterHooks) *Simulator {
	intensity, ok := ParseIntensity(string(cfg.Intensity))
	if !ok || intensity == "" {
		intensity = IntensityNormal
	}
	if hooks.ProxyURL == "" {
		hooks.ProxyURL = "http://localhost:8080"
	}
	hooks.ProxyURL = strings.TrimRight(hooks.ProxyURL, "/")

	if hooks.TickInterval <= 0 {
		hooks.TickInterval = defaultTickMs
	}
	if hooks.BaselineHoldMin <= 0 {
		hooks.BaselineHoldMin = 9 * time.Second
	}
	if hooks.BaselineHoldMax < hooks.BaselineHoldMin {
		hooks.BaselineHoldMax = hooks.BaselineHoldMin + 6*time.Second
	}
	if hooks.WaveDurationMin <= 0 {
		hooks.WaveDurationMin = 5 * time.Second
	}
	if hooks.WaveDurationMax < hooks.WaveDurationMin {
		hooks.WaveDurationMax = hooks.WaveDurationMin + 4*time.Second
	}

	httpClient := &http.Client{
		Timeout: 6 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        128,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	s := &Simulator{
		hooks:      hooks,
		httpClient: httpClient,
		intensity:  intensity,
		allowChaos: cfg.AllowChaos,
		allowDDoS:  cfg.AllowDDoS,
		activeWave: WaveIdle,
		waveLabel:  "Idle",
		waveDetail: "Simulator paused. Start from Overview or Lab Studio to stream real-life mixed traffic.",
		wakeupCh:   make(chan struct{}, 1),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		sem:        make(chan struct{}, maxInFlight),
	}

	if s.hooks.SetBackendDelay == nil {
		s.hooks.SetBackendDelay = s.defaultSetBackendDelay
	}
	if s.hooks.ToggleBackendHealth == nil {
		s.hooks.ToggleBackendHealth = s.defaultToggleBackendHealth
	}

	return s
}

func (s *Simulator) defaultSetBackendDelay(targetURL string, ms int64) error {
	if ms < 0 {
		ms = 0
	}
	delayEndpoint := fmt.Sprintf("%s/chaos/delay?ms=%d", strings.TrimRight(targetURL, "/"), ms)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(delayEndpoint)
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			err = fmt.Errorf("chaos delay returned HTTP %d", resp.StatusCode)
		}
	}
	if s.hooks.GetBackends != nil {
		for _, b := range s.hooks.GetBackends() {
			if b.URL.String() == targetURL {
				b.SetSimulatedDelay(ms)
			}
		}
	}
	return err
}

func (s *Simulator) defaultToggleBackendHealth(targetURL, state string) error {
	endpoint := fmt.Sprintf("%s/chaos/toggle?state=%s", strings.TrimRight(targetURL, "/"), state)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("chaos toggle returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// Start begins autonomous background traffic generation and wave scheduling.
func (s *Simulator) Start() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.running = true
	s.cancel = cancel
	s.activeWave = WaveBaseline
	s.waveLabel = "Organic mix"
	s.waveDetail = "Streaming randomized public IPs, sticky sessions, slow queries, and canary traffic."
	s.mu.Unlock()

	s.recordEvent(WaveBaseline, "Simulation started", "Streaming mixed unique IPs, sticky sessions, and randomized concurrency.", "ok")

	s.wg.Add(2)
	go s.trafficLoop(ctx)
	go s.directorLoop(ctx)
}

// Stop halts traffic loops, waits for in-flight requests, and reverts any active chaos state.
func (s *Simulator) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	cancel := s.cancel
	s.running = false
	s.cancel = nil
	s.activeWave = WaveIdle
	s.waveLabel = "Idle"
	s.waveDetail = "Simulator stopped and reversible chaos mutations restored."
	s.targetRPS = 0
	s.attackerIPs = nil
	s.pinnedIP = ""
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	s.wg.Wait()

	s.restoreChaos(true)
	s.recordEvent(WaveIdle, "Simulation stopped", "Restored cluster baseline weights, delays, canary flags, and healthy state.", "info")
}

// Configure updates intensity and chaos/DDoS policy live without interrupting execution.
func (s *Simulator) Configure(intensity *Intensity, allowChaos, allowDDoS *bool) {
	s.mu.Lock()
	if intensity != nil {
		if norm, ok := ParseIntensity(string(*intensity)); ok {
			s.intensity = norm
		}
	}
	if allowChaos != nil {
		s.allowChaos = *allowChaos
	}
	if allowDDoS != nil {
		s.allowDDoS = *allowDDoS
	}
	chaosAllowed := s.allowChaos
	s.mu.Unlock()

	if !chaosAllowed {
		s.restoreChaos(false)
	}
}

// TriggerWave queues an immediate scenario wave, starting the simulator if paused.
func (s *Simulator) TriggerWave(kind WaveKind) error {
	if _, ok := ParseWave(string(kind)); !ok {
		return fmt.Errorf("unsupported simulation wave %q", kind)
	}
	if !s.IsRunning() {
		s.Start()
	}

	s.triggerMux.Lock()
	s.pendingTrigger = kind
	s.triggerMux.Unlock()

	select {
	case s.wakeupCh <- struct{}{}:
	default:
	}
	return nil
}

func (s *Simulator) takePendingTrigger() WaveKind {
	s.triggerMux.Lock()
	defer s.triggerMux.Unlock()
	kind := s.pendingTrigger
	s.pendingTrigger = ""
	select {
	case <-s.wakeupCh:
	default:
	}
	return kind
}

// IsRunning reports whether background traffic loops are active.
func (s *Simulator) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.running
}

// Reset clears observed simulation counters and event history without altering config.
func (s *Simulator) Reset() {
	atomic.StoreInt64(&s.totalSent, 0)
	atomic.StoreInt64(&s.uniqueIPReqs, 0)
	atomic.StoreInt64(&s.repeatIPReqs, 0)
	atomic.StoreInt64(&s.canaryReqs, 0)
	atomic.StoreInt64(&s.slowReqs, 0)
	atomic.StoreInt64(&s.attackReqs, 0)
	atomic.StoreInt64(&s.blocked429, 0)
	atomic.StoreInt64(&s.chaosEvents, 0)

	s.mu.Lock()
	s.events = nil
	s.mu.Unlock()
}

// Snapshot returns an immutable point-in-time state summary for the dashboard API.
func (s *Simulator) Snapshot() Snapshot {
	s.mu.RLock()
	eventsCopy := make([]SimEvent, len(s.events))
	copy(eventsCopy, s.events)
	snap := Snapshot{
		Running:           s.running,
		Intensity:         s.intensity,
		AllowChaos:        s.allowChaos,
		AllowDDoS:         s.allowDDoS,
		ActiveWave:        s.activeWave,
		WaveLabel:         s.waveLabel,
		WaveDetail:        s.waveDetail,
		TargetRPS:         s.targetRPS,
		ActiveAttackerIPs: len(s.attackerIPs),
		Counters: SimCounters{
			TotalSent:    atomic.LoadInt64(&s.totalSent),
			UniqueIPReqs: atomic.LoadInt64(&s.uniqueIPReqs),
			RepeatIPReqs: atomic.LoadInt64(&s.repeatIPReqs),
			CanaryReqs:   atomic.LoadInt64(&s.canaryReqs),
			SlowReqs:     atomic.LoadInt64(&s.slowReqs),
			AttackReqs:   atomic.LoadInt64(&s.attackReqs),
			Blocked429:   atomic.LoadInt64(&s.blocked429),
			ChaosEvents:  atomic.LoadInt64(&s.chaosEvents),
		},
		Events: eventsCopy,
	}
	s.mu.RUnlock()
	return snap
}

func (s *Simulator) recordEvent(wave WaveKind, title, detail, severity string) {
	ev := SimEvent{
		Timestamp: time.Now().Format("15:04:05"),
		Wave:      wave,
		Title:     title,
		Detail:    detail,
		Severity:  severity,
	}
	s.mu.Lock()
	s.events = append([]SimEvent{ev}, s.events...)
	if len(s.events) > maxSimEvents {
		s.events = s.events[:maxSimEvents]
	}
	s.mu.Unlock()
}

func (s *Simulator) setWaveState(wave WaveKind, label, detail string, attackers []string, pinnedIP string) {
	s.mu.Lock()
	s.activeWave = wave
	s.waveLabel = label
	s.waveDetail = detail
	s.attackerIPs = attackers
	s.pinnedIP = pinnedIP
	s.mu.Unlock()
}

func (s *Simulator) randIntn(n int) int {
	if n <= 1 {
		return 0
	}
	s.rngMux.Lock()
	val := s.rng.Intn(n)
	s.rngMux.Unlock()
	return val
}

func (s *Simulator) randFloat() float64 {
	s.rngMux.Lock()
	val := s.rng.Float64()
	s.rngMux.Unlock()
	return val
}

func (s *Simulator) randomPublicIP() string {
	pref := publicPrefixes[s.randIntn(len(publicPrefixes))]
	return fmt.Sprintf("%d.%d.%d.%d", pref[0], pref[1], s.randIntn(250)+1, s.randIntn(253)+1)
}

func (s *Simulator) randomDuration(minD, maxD time.Duration) time.Duration {
	if maxD <= minD {
		return minD
	}
	span := maxD - minD
	return minD + time.Duration(s.randFloat()*float64(span))
}

func (s *Simulator) intensityMultiplier() (baseRPS, attackRPS float64) {
	s.mu.RLock()
	intensity := s.intensity
	s.mu.RUnlock()

	switch intensity {
	case IntensityLow:
		return 6.0, 22.0
	case IntensityHeavy:
		return 28.0, 70.0
	default:
		return 14.0, 40.0
	}
}

func (s *Simulator) trafficLoop(ctx context.Context) {
	defer s.wg.Done()

	var fracCarry float64
	burstSessionLeft := 0
	burstSessionIP := ""

	for {
		tickInterval := time.Duration(float64(s.hooks.TickInterval) * (0.75 + s.randFloat()*0.5))
		timer := time.NewTimer(tickInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		s.mu.RLock()
		wave := s.activeWave
		attackers := append([]string(nil), s.attackerIPs...)
		pinned := s.pinnedIP
		s.mu.RUnlock()

		baseRPS, attackRPS := s.intensityMultiplier()
		// Add smooth organic jitter (0.65x to 1.45x)
		organicRPS := baseRPS * (0.65 + s.randFloat()*0.80)

		var extraAttackRPS float64
		if wave == WaveSingleDoS || wave == WaveBotnetDDoS {
			extraAttackRPS = attackRPS * (0.85 + s.randFloat()*0.35)
		} else if wave == WaveLatency {
			organicRPS *= 1.35
		}

		totalRPS := organicRPS + extraAttackRPS
		s.mu.Lock()
		if s.running {
			s.targetRPS = totalRPS
		}
		s.mu.Unlock()

		sec := tickInterval.Seconds()
		organicFloat := organicRPS*sec + fracCarry
		organicCount := int(organicFloat)
		fracCarry = organicFloat - float64(organicCount)

		attackCount := int(extraAttackRPS*sec + s.randFloat())

		// Occasionally start a short repeat-client micro-burst in organic mode so real sessions look bursty
		if wave == WaveBaseline && burstSessionLeft <= 0 && s.randFloat() < 0.14 {
			burstSessionLeft = 2 + s.randIntn(5)
			burstSessionIP = stickyClientIPs[s.randIntn(len(stickyClientIPs))]
		}

		for i := 0; i < organicCount; i++ {
			if ctx.Err() != nil {
				return
			}
			useSticky := false
			clientIP := ""
			if wave == WaveSticky && pinned != "" && s.randFloat() < 0.88 {
				useSticky = true
				clientIP = pinned
			} else if burstSessionLeft > 0 && s.randFloat() < 0.70 {
				useSticky = true
				clientIP = burstSessionIP
				burstSessionLeft--
			} else if s.randFloat() < 0.30 {
				useSticky = true
				clientIP = stickyClientIPs[s.randIntn(len(stickyClientIPs))]
			} else {
				clientIP = s.randomPublicIP()
			}

			canaryRoll := 0.06
			if wave == WaveCanaryWRR {
				canaryRoll = 0.24
			}
			isCanary := s.randFloat() < canaryRoll

			delayMs := 0
			if wave == WaveLatency && s.randFloat() < 0.28 {
				delayMs = 240 + s.randIntn(180)
			} else if s.randFloat() < 0.08 {
				delayMs = 90 + s.randIntn(150)
			}

			s.dispatchAsync(ctx, clientIP, useSticky, isCanary, false, delayMs)
		}

		for i := 0; i < attackCount && len(attackers) > 0; i++ {
			if ctx.Err() != nil {
				return
			}
			attackerIP := attackers[s.randIntn(len(attackers))]
			s.dispatchAsync(ctx, attackerIP, true, false, true, 0)
		}
	}
}

func (s *Simulator) dispatchAsync(ctx context.Context, clientIP string, isRepeatIP, isCanary, isAttack bool, delayMs int) {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return
	default:
		// Backpressure: drop excess simulated burst packet if in-flight cap reached
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { <-s.sem }()

		route := organicRoutes[s.randIntn(len(organicRoutes))]
		method := route.method
		path := route.path
		if isAttack {
			if s.randFloat() < 0.7 {
				method = http.MethodGet
				path = "/api/search"
			} else {
				method = http.MethodPost
				path = "/api/checkout"
			}
		}

		reqURL := s.hooks.ProxyURL + path
		if delayMs > 0 {
			reqURL += "?delay=" + strconv.Itoa(delayMs)
		}

		var bodyReader io.Reader
		if method == http.MethodPost {
			bodyReader = strings.NewReader(`{"sim":true}`)
		}

		req, err := http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
		if err != nil {
			return
		}
		if method == http.MethodPost {
			req.Header.Set("Content-Type", "application/json")
			if route.idem && !isAttack {
				req.Header.Set("Idempotency-Key", fmt.Sprintf("sim-%d", time.Now().UnixNano()))
			}
		}
		req.Header.Set("X-Forwarded-For", clientIP)

		if isCanary {
			mode := s.randIntn(3)
			switch mode {
			case 0:
				req.Header.Set("X-Canary", "true")
			case 1:
				if strings.Contains(req.URL.RawQuery, "delay=") {
					req.URL.RawQuery += "&canary=true"
				} else {
					req.URL.RawQuery = "canary=true"
				}
			default:
				req.AddCookie(&http.Cookie{Name: "canary", Value: "true"})
			}
		}

		atomic.AddInt64(&s.totalSent, 1)
		if isAttack {
			atomic.AddInt64(&s.attackReqs, 1)
		} else if isRepeatIP {
			atomic.AddInt64(&s.repeatIPReqs, 1)
		} else {
			atomic.AddInt64(&s.uniqueIPReqs, 1)
		}
		if isCanary {
			atomic.AddInt64(&s.canaryReqs, 1)
		}
		if delayMs > 0 {
			atomic.AddInt64(&s.slowReqs, 1)
		}

		resp, err := s.httpClient.Do(req)
		if err != nil {
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			atomic.AddInt64(&s.blocked429, 1)
		}
	}()
}

func (s *Simulator) directorLoop(ctx context.Context) {
	defer s.wg.Done()
	var lastSpecial WaveKind

	for {
		var nextWave WaveKind
		if manual := s.takePendingTrigger(); manual != "" {
			nextWave = manual
		} else {
			hold := s.randomDuration(s.hooks.BaselineHoldMin, s.hooks.BaselineHoldMax)
			timer := time.NewTimer(hold)

			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-s.wakeupCh:
				timer.Stop()
				if manual := s.takePendingTrigger(); manual != "" {
					nextWave = manual
				} else {
					nextWave = s.pickNextWave(lastSpecial)
				}
			case <-timer.C:
				nextWave = s.pickNextWave(lastSpecial)
			}
		}

		if nextWave == "" || nextWave == WaveBaseline {
			s.setWaveState(WaveBaseline, "Organic mix", "Streaming randomized public IPs, sticky sessions, slow queries, and canary traffic.", nil, "")
			continue
		}

		lastSpecial = nextWave
		s.executeWave(ctx, nextWave)

		if ctx.Err() != nil {
			return
		}
		s.setWaveState(WaveBaseline, "Organic mix", "Returned to mixed organic baseline across random and returning clients.", nil, "")
	}
}

func (s *Simulator) pickNextWave(prev WaveKind) WaveKind {
	s.mu.RLock()
	chaos := s.allowChaos
	ddos := s.allowDDoS
	s.mu.RUnlock()

	candidates := []WaveKind{WaveSticky}
	if ddos {
		candidates = append(candidates, WaveSingleDoS, WaveBotnetDDoS, WaveSingleDoS)
	}
	if chaos {
		candidates = append(candidates, WaveLatency, WaveCanaryWRR, WaveNodeOutage)
	}

	if len(candidates) == 1 {
		return candidates[0]
	}
	filtered := candidates[:0]
	for _, c := range candidates {
		if c != prev {
			filtered = append(filtered, c)
		}
	}
	if len(filtered) == 0 {
		return candidates[s.randIntn(len(candidates))]
	}
	return filtered[s.randIntn(len(filtered))]
}

func (s *Simulator) executeWave(ctx context.Context, wave WaveKind) {
	dur := s.randomDuration(s.hooks.WaveDurationMin, s.hooks.WaveDurationMax)

	switch wave {
	case WaveSticky:
		pinned := stickyClientIPs[s.randIntn(len(stickyClientIPs))]
		detail := fmt.Sprintf("Concentrating returning client sessions from %s (tests IP Hash & Smart Auto affinity).", pinned)
		s.setWaveState(WaveSticky, "Sticky session burst", detail, nil, pinned)
		s.recordEvent(WaveSticky, "Sticky client session burst", detail, "info")
		s.waitOrAbort(ctx, dur)

	case WaveSingleDoS:
		attacker := fmt.Sprintf("45.155.205.%d", 50+s.randIntn(180))
		detail := fmt.Sprintf("High-rate flood from %s competing with ongoing random public users.", attacker)
		s.setWaveState(WaveSingleDoS, "Single-IP DoS flood", detail, []string{attacker}, "")
		s.recordEvent(WaveSingleDoS, "Single-IP DoS wave launched", detail, "warn")
		s.waitOrAbort(ctx, dur)

	case WaveBotnetDDoS:
		count := 5 + s.randIntn(4)
		swarm := make([]string, 0, count)
		for i := 0; i < count; i++ {
			swarm = append(swarm, fmt.Sprintf("185.220.101.%d", 10+s.randIntn(220)))
		}
		detail := fmt.Sprintf("Coordinated %d-IP botnet flood probing Edge Shield buckets across endpoints.", len(swarm))
		s.setWaveState(WaveBotnetDDoS, "Botnet DDoS wave", detail, swarm, "")
		s.recordEvent(WaveBotnetDDoS, "Botnet DDoS wave launched", detail, "bad")
		s.waitOrAbort(ctx, dur)

	case WaveLatency:
		s.runLatencySpikeWave(ctx, dur)

	case WaveCanaryWRR:
		s.runCanaryWeightedWave(ctx, dur)

	case WaveNodeOutage:
		s.runNodeOutageWave(ctx, dur)
	}
}

func (s *Simulator) waitOrAbort(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-s.wakeupCh:
	case <-t.C:
	}
}

func (s *Simulator) ensureBackupLocked() *chaosBackup {
	if s.backup == nil {
		s.backup = newChaosBackup()
	}
	return s.backup
}

func (s *Simulator) runLatencySpikeWave(ctx context.Context, dur time.Duration) {
	if s.hooks.GetBackends == nil {
		s.waitOrAbort(ctx, dur)
		return
	}
	backends := s.hooks.GetBackends()
	var target *backend.Backend
	for _, b := range backends {
		if b.IsAlive() {
			target = b
			break
		}
	}
	if target == nil {
		s.waitOrAbort(ctx, dur)
		return
	}

	urlStr := target.URL.String()
	lagMs := int64(240 + s.randIntn(140))

	s.chaosMux.Lock()
	bk := s.ensureBackupLocked()
	if _, exists := bk.delays[urlStr]; !exists {
		bk.delays[urlStr] = target.GetSimulatedDelay()
	}
	s.chaosMux.Unlock()

	atomic.AddInt64(&s.chaosEvents, 1)
	if s.hooks.SetBackendDelay != nil {
		_ = s.hooks.SetBackendDelay(urlStr, lagMs)
	} else {
		target.SetSimulatedDelay(lagMs)
	}

	detail := fmt.Sprintf("Injected %d ms congestion delay on %s to trigger Least Connections skew routing.", lagMs, strings.TrimPrefix(urlStr, "http://"))
	s.setWaveState(WaveLatency, "Latency chaos spike", detail, nil, "")
	s.recordEvent(WaveLatency, "Upstream congestion injected", detail, "warn")

	s.waitOrAbort(ctx, dur)
	s.restoreChaos(false)
}

func (s *Simulator) runCanaryWeightedWave(ctx context.Context, dur time.Duration) {
	if s.hooks.GetBackends == nil {
		s.waitOrAbort(ctx, dur)
		return
	}
	backends := s.hooks.GetBackends()
	var healthy []*backend.Backend
	for _, b := range backends {
		if b.IsAlive() {
			healthy = append(healthy, b)
		}
	}
	if len(healthy) < 2 {
		s.waitOrAbort(ctx, dur)
		return
	}

	s.chaosMux.Lock()
	bk := s.ensureBackupLocked()
	for _, b := range backends {
		u := b.URL.String()
		if _, exists := bk.weights[u]; !exists {
			bk.weights[u] = b.GetWeight()
		}
		if _, exists := bk.canary[u]; !exists {
			bk.canary[u] = b.IsCanaryNode()
			bk.canaryTag[u] = b.GetCanaryTag()
		}
	}
	s.chaosMux.Unlock()

	atomic.AddInt64(&s.chaosEvents, 1)

	canaryIdx := len(healthy) - 1
	for i, b := range healthy {
		if i == canaryIdx {
			b.SetWeight(1)
			b.SetCanary(true, "v2.0-canary")
		} else {
			b.SetWeight(5 - int64(i%2))
			b.SetCanary(false, "")
		}
	}

	canaryHost := strings.TrimPrefix(healthy[canaryIdx].URL.String(), "http://")
	detail := fmt.Sprintf("Weighted capacity split + %s tagged v2.0-canary while streaming 24%% canary clients.", canaryHost)
	s.setWaveState(WaveCanaryWRR, "Canary + Weighted split", detail, nil, "")
	s.recordEvent(WaveCanaryWRR, "Canary rollout split active", detail, "info")

	s.waitOrAbort(ctx, dur)
	s.restoreChaos(false)
}

func (s *Simulator) runNodeOutageWave(ctx context.Context, dur time.Duration) {
	if s.hooks.GetBackends == nil {
		s.waitOrAbort(ctx, dur)
		return
	}
	backends := s.hooks.GetBackends()
	var healthy []*backend.Backend
	for _, b := range backends {
		if b.IsAlive() && b.IsUp() {
			healthy = append(healthy, b)
		}
	}
	if len(healthy) < 2 {
		s.waitOrAbort(ctx, dur)
		return
	}

	target := healthy[s.randIntn(len(healthy))]
	urlStr := target.URL.String()

	s.chaosMux.Lock()
	bk := s.ensureBackupLocked()
	bk.downURLs[urlStr] = true
	if s.touchedCircuitURLs == nil {
		s.touchedCircuitURLs = make(map[string]bool)
	}
	s.touchedCircuitURLs[urlStr] = true
	s.chaosMux.Unlock()

	atomic.AddInt64(&s.chaosEvents, 1)
	if s.hooks.ToggleBackendHealth != nil {
		if err := s.hooks.ToggleBackendHealth(urlStr, "down"); err != nil {
			// Fall back safely to marking backend offline if mock /chaos endpoint isn't present
			target.SetAlive(false)
		} else if s.hooks.CheckHealth != nil {
			s.hooks.CheckHealth()
		}
	} else {
		target.SetAlive(false)
	}

	host := strings.TrimPrefix(urlStr, "http://")
	detail := fmt.Sprintf("Fault injected on %s; health checks, circuit breaker, and peer failover absorb live traffic.", host)
	s.setWaveState(WaveNodeOutage, "Node outage drill", detail, nil, "")
	s.recordEvent(WaveNodeOutage, "Reversible node outage started", detail, "bad")

	s.waitOrAbort(ctx, dur)
	s.restoreChaos(false)
}

func (s *Simulator) restoreChaos(resetCircuitBreakers bool) {
	s.chaosMux.Lock()
	bk := s.backup
	s.backup = nil
	var touched map[string]bool
	if resetCircuitBreakers && len(s.touchedCircuitURLs) > 0 {
		touched = s.touchedCircuitURLs
		s.touchedCircuitURLs = nil
	}
	s.chaosMux.Unlock()

	if bk == nil && len(touched) == 0 {
		return
	}

	backendsByURL := make(map[string]*backend.Backend)
	if s.hooks.GetBackends != nil {
		for _, b := range s.hooks.GetBackends() {
			backendsByURL[b.URL.String()] = b
		}
	}

	for u := range touched {
		if b, ok := backendsByURL[u]; ok && b.CircuitBreaker != nil {
			b.CircuitBreaker.Reset()
		}
	}

	if bk == nil {
		return
	}

	for u, origDelay := range bk.delays {
		if s.hooks.SetBackendDelay != nil {
			_ = s.hooks.SetBackendDelay(u, origDelay)
		}
		if b, ok := backendsByURL[u]; ok {
			b.SetSimulatedDelay(origDelay)
		}
	}

	for u, origWeight := range bk.weights {
		if b, ok := backendsByURL[u]; ok {
			b.SetWeight(origWeight)
		}
	}

	for u, origCanary := range bk.canary {
		if b, ok := backendsByURL[u]; ok {
			b.SetCanary(origCanary, bk.canaryTag[u])
		}
	}

	restoredNode := false
	for u := range bk.downURLs {
		restoredNode = true
		if s.hooks.ToggleBackendHealth != nil {
			_ = s.hooks.ToggleBackendHealth(u, "up")
		}
		if b, ok := backendsByURL[u]; ok {
			b.SetAlive(true)
		}
	}

	if restoredNode && s.hooks.CheckHealth != nil {
		s.hooks.CheckHealth()
	}

	if len(bk.delays) > 0 || len(bk.weights) > 0 || len(bk.canary) > 0 || len(bk.downURLs) > 0 {
		s.recordEvent(WaveBaseline, "Chaos state restored", "Reverted temporary latency, weights, canary tags, and node health back to baseline.", "ok")
	}
}
