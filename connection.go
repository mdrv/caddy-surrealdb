package caddysurrealdb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"go.uber.org/zap"
)

// AuthLevel controls which SurrealDB scope a credential authenticates against.
type AuthLevel string

const (
	AuthRoot      AuthLevel = "root"
	AuthNamespace AuthLevel = "namespace"
	AuthDatabase  AuthLevel = "database"
	AuthRecord    AuthLevel = "record"
	AuthToken     AuthLevel = "token"
)

// AuthConfig describes how caddy-surrealdb authenticates to SurrealDB.
//
// The level determines which fields are required:
//
//   - root       — Username + Password
//   - namespace  — Username + Password (auth against Manager.Namespace)
//   - database   — Username + Password (auth against Namespace + Database)
//   - record     — Access (DEFINE ACCESS method name) + BearerKey (surreal-bearer-...)
//     OR Token (a previously issued record JWT)
//   - token      — Token (any pre-issued JWT; used via Authenticate, no refresh)
type AuthConfig struct {
	Level     AuthLevel `json:"level,omitempty"`
	Username  string    `json:"username,omitempty"`
	Password  string    `json:"password,omitempty"`
	Access    string    `json:"access,omitempty"`
	Token     string    `json:"token,omitempty"`
	BearerKey string    `json:"bearer_key,omitempty"`
}

// Manager maintains a live SurrealDB connection with proactive token refresh
// (default: re-sign-in when <5 minutes of TTL remain) and exponential-backoff
// reconnection. All methods are goroutine-safe.
//
// The supervisor strategy mirrors the kotaete two-layer pattern:
//
//  1. Proactive — heartbeat goroutine watches the JWT exp claim and re-signs-in
//     before the server can revoke the token.
//  2. Reactive  — three consecutive heartbeat failures force a full reconnect
//     with exponential backoff (1s → 2s → 4s → ... → 30s max).
type Manager struct {
	endpoint string
	ns       string
	db       string
	auth     AuthConfig
	logger   *zap.Logger

	tokenRefresh      time.Duration
	heartbeatInterval time.Duration
	heartbeatTable    string
	heartbeatSchema   *sync.Once
	instanceName      string
	needsWS           bool

	mu        sync.RWMutex
	conn      *surrealdb.DB
	token     string
	tokenExp  time.Time
	failures  int
	connected bool

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	ready   chan struct{}
	readyOk bool
}

// Option configures a Manager.
type Option func(*Manager)

// WithTokenRefresh sets the proactive refresh threshold (default 5m).
func WithTokenRefresh(d time.Duration) Option { return func(m *Manager) { m.tokenRefresh = d } }

// WithHeartbeat sets the heartbeat interval (default 30s).
func WithHeartbeat(d time.Duration) Option { return func(m *Manager) { m.heartbeatInterval = d } }

// WithHeartbeatTable enables UPSERT-based heartbeats to the named table.
// Empty (the default) disables status-row writes — the liveness Version()
// ping still fires every HeartbeatInterval, but no rows are added to the
// user's database. Opt in only when you need cross-client visibility.
func WithHeartbeatTable(t string) Option { return func(m *Manager) { m.heartbeatTable = t } }

// WithInstanceName sets the row id used for heartbeat UPSERTs.
func WithInstanceName(n string) Option { return func(m *Manager) { m.instanceName = n } }

// WithLogger injects a zap logger.
func WithLogger(l *zap.Logger) Option { return func(m *Manager) { m.logger = l } }

// WithNeedsWS forces the connection onto a WebSocket transport (required for
// live queries). http(s):// endpoints are rewritten to ws(wss)://.
func WithNeedsWS(b bool) Option { return func(m *Manager) { m.needsWS = b } }

// NewManager constructs a Manager with the given options.
func NewManager(endpoint, ns, dbName string, auth AuthConfig, opts ...Option) *Manager {
	m := &Manager{
		endpoint:          endpoint,
		ns:                ns,
		db:                dbName,
		auth:              auth,
		tokenRefresh:      5 * time.Minute,
		heartbeatInterval: 30 * time.Second,
		heartbeatTable:    "", // opt-in via WithHeartbeatTable; default keeps user DB clean
		heartbeatSchema:   &sync.Once{},
		instanceName:      fmt.Sprintf("caddy-%d", time.Now().UnixNano()),
		ready:             make(chan struct{}),
	}
	for _, o := range opts {
		o(m)
	}
	if m.logger == nil {
		m.logger = zap.NewNop()
	}
	return m
}

// Start launches the supervisor goroutine. It returns immediately; callers
// should AwaitReady to block on first successful connect.
func (m *Manager) Start(parent context.Context) error {
	m.ctx, m.cancel = context.WithCancel(parent)
	m.wg.Add(1)
	go m.run()
	return nil
}

// Stop gracefully terminates the supervisor and closes the connection.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	m.mu.Lock()
	if m.conn != nil {
		_ = m.conn.Close(context.Background())
		m.conn = nil
	}
	m.connected = false
	m.mu.Unlock()
}

// AwaitReady blocks until the first successful connection, manager stop, or
// ctx cancellation.
func (m *Manager) AwaitReady(ctx context.Context) error {
	select {
	case <-m.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-m.ctx.Done():
		return fmt.Errorf("surrealdb: manager stopped before ready")
	}
}

// Get returns the active connection, blocking until ready.
func (m *Manager) Get(ctx context.Context) (*surrealdb.DB, error) {
	if err := m.AwaitReady(ctx); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.conn == nil {
		return nil, fmt.Errorf("surrealdb: not connected")
	}
	return m.conn, nil
}

// Token returns the current session JWT (for debugging / passthrough).
func (m *Manager) Token() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.token
}

// run is the supervisor: connect → supervise → reconnect, with exponential
// backoff. Runs until ctx is cancelled.
func (m *Manager) run() {
	defer m.wg.Done()

	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if m.ctx.Err() != nil {
			return
		}
		conn, token, err := m.dial(m.ctx)
		if err != nil {
			m.logger.Warn("surrealdb: connect failed",
				zap.Error(err), zap.Duration("backoff", backoff))
			select {
			case <-time.After(backoff):
			case <-m.ctx.Done():
				return
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}

		backoff = time.Second
		exp := parseTokenExpiry(token, m.logger)
		m.mu.Lock()
		m.conn = conn
		m.token = token
		m.tokenExp = exp
		m.failures = 0
		m.connected = true
		m.mu.Unlock()

		m.logger.Info("surrealdb: connected",
			zap.String("ns", m.ns), zap.String("db", m.db),
			zap.String("endpoint", m.endpoint),
			zap.Time("token_exp", exp))

		m.signalReady()
		m.supervise()

		// Connection lost; tear down and reconnect.
		m.mu.Lock()
		old := m.conn
		m.conn = nil
		m.token = ""
		m.tokenExp = time.Time{}
		m.connected = false
		m.mu.Unlock()
		if old != nil {
			_ = old.Close(context.Background())
		}
	}
}

func (m *Manager) signalReady() {
	m.mu.Lock()
	if !m.readyOk {
		m.readyOk = true
		close(m.ready)
	}
	m.mu.Unlock()
}

// dial performs a single connect attempt: FromEndpointURLString → SignIn (or
// Authenticate) → Use.
func (m *Manager) dial(ctx context.Context) (*surrealdb.DB, string, error) {
	endpoint := m.endpoint
	if m.needsWS {
		endpoint = ensureWSScheme(endpoint)
	}
	conn, err := surrealdb.FromEndpointURLString(ctx, endpoint)
	if err != nil {
		return nil, "", fmt.Errorf("dial %q: %w", endpoint, err)
	}

	token, err := m.signIn(ctx, conn)
	if err != nil {
		_ = conn.Close(ctx)
		return nil, "", err
	}
	if err := conn.Use(ctx, m.ns, m.db); err != nil {
		_ = conn.Close(ctx)
		return nil, "", fmt.Errorf("use %s/%s: %w", m.ns, m.db, err)
	}
	return conn, token, nil
}

// signIn routes to the correct auth flow for the configured level.
func (m *Manager) signIn(ctx context.Context, conn *surrealdb.DB) (string, error) {
	switch m.auth.Level {
	case AuthToken:
		if err := conn.Authenticate(ctx, m.auth.Token); err != nil {
			return "", fmt.Errorf("authenticate: %w", err)
		}
		return m.auth.Token, nil

	case AuthRoot:
		t, err := conn.SignIn(ctx, map[string]any{
			"user": m.auth.Username, "pass": m.auth.Password,
		})
		if err != nil {
			return "", fmt.Errorf("signin root: %w", err)
		}
		return t, nil

	case AuthNamespace:
		t, err := conn.SignIn(ctx, map[string]any{
			"user": m.auth.Username, "pass": m.auth.Password, "NS": m.ns,
		})
		if err != nil {
			return "", fmt.Errorf("signin namespace: %w", err)
		}
		return t, nil

	case AuthDatabase:
		t, err := conn.SignIn(ctx, map[string]any{
			"user": m.auth.Username, "pass": m.auth.Password,
			"NS": m.ns, "DB": m.db,
		})
		if err != nil {
			return "", fmt.Errorf("signin database: %w", err)
		}
		return t, nil

	case AuthRecord:
		auth := map[string]any{"NS": m.ns, "DB": m.db, "AC": m.auth.Access}
		switch {
		case m.auth.BearerKey != "":
			auth["key"] = m.auth.BearerKey
		case m.auth.Token != "":
			// Re-use an issued record JWT.
			if err := conn.Authenticate(ctx, m.auth.Token); err != nil {
				return "", fmt.Errorf("authenticate record: %w", err)
			}
			return m.auth.Token, nil
		default:
			return "", fmt.Errorf("record auth requires bearer_key or token")
		}
		t, err := conn.SignIn(ctx, auth)
		if err != nil {
			return "", fmt.Errorf("signin record: %w", err)
		}
		return t, nil
	}
	return "", fmt.Errorf("unknown auth level %q", m.auth.Level)
}

// supervise runs the heartbeat+refresh loop until ctx cancels or 3 consecutive
// heartbeat failures force a reconnect.
func (m *Manager) supervise() {
	tick := time.NewTicker(m.heartbeatInterval)
	defer tick.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-tick.C:
			if m.maybeRefresh() {
				continue
			}
			if err := m.heartbeat(); err != nil {
				m.logger.Warn("surrealdb: heartbeat failed",
					zap.Error(err), zap.Int("failures", m.failures+1))
				m.mu.Lock()
				m.failures++
				fatal := m.failures >= 3
				m.mu.Unlock()
				if fatal {
					m.logger.Warn("surrealdb: heartbeat threshold reached; reconnecting")
					return
				}
			} else {
				m.mu.Lock()
				m.failures = 0
				m.mu.Unlock()
			}
		}
	}
}

// maybeRefresh re-signs-in when token TTL drops below the threshold. Errors
// are logged but NOT propagated (the reactive layer in supervise will catch
// persistent failures via heartbeat).
func (m *Manager) maybeRefresh() bool {
	if m.auth.Level == AuthToken {
		return false // no credentials to refresh with
	}
	m.mu.RLock()
	exp := m.tokenExp
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil || exp.IsZero() {
		return false
	}
	remaining := time.Until(exp)
	if remaining > m.tokenRefresh {
		return false
	}
	m.logger.Info("surrealdb: refreshing token",
		zap.Duration("remaining", remaining), zap.String("level", string(m.auth.Level)))

	token, err := m.signIn(m.ctx, conn)
	if err != nil {
		m.logger.Warn("surrealdb: token refresh failed", zap.Error(err))
		return false
	}
	m.mu.Lock()
	m.token = token
	m.tokenExp = parseTokenExpiry(token, m.logger)
	m.mu.Unlock()
	return true
}

// heartbeat issues a Version() ping and (optionally) UPSERTs a status row so
// stale instances can be detected from inside SurrealDB.
func (m *Manager) heartbeat() error {
	m.mu.RLock()
	conn := m.conn
	m.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("not connected")
	}
	if _, err := conn.Version(m.ctx); err != nil {
		return fmt.Errorf("version ping: %w", err)
	}
	if m.heartbeatTable == "" {
		return nil
	}

	// Lazily define the heartbeat table once per Manager lifetime. SCHEMALESS
	// + typed FIELDs gives type safety on known columns while tolerating
	// user additions. IF NOT EXISTS preserves any user override from the
	// Schema block.
	m.heartbeatSchema.Do(func() {
		stmts := []string{
			"DEFINE TABLE IF NOT EXISTS " + ident(m.heartbeatTable) + " SCHEMALESS PERMISSIONS FOR select, create, update WHERE true;",
			"DEFINE FIELD IF NOT EXISTS status            ON " + ident(m.heartbeatTable) + " TYPE string;",
			"DEFINE FIELD IF NOT EXISTS last_heartbeat_at ON " + ident(m.heartbeatTable) + " TYPE datetime;",
			"DEFINE FIELD IF NOT EXISTS started_at        ON " + ident(m.heartbeatTable) + " TYPE datetime;",
			"DEFINE FIELD IF NOT EXISTS endpoint          ON " + ident(m.heartbeatTable) + " TYPE string;",
			"DEFINE FIELD IF NOT EXISTS ns                ON " + ident(m.heartbeatTable) + " TYPE string;",
			"DEFINE FIELD IF NOT EXISTS db                ON " + ident(m.heartbeatTable) + " TYPE string;",
			"DEFINE FIELD IF NOT EXISTS level             ON " + ident(m.heartbeatTable) + " TYPE string;",
			"DEFINE FIELD IF NOT EXISTS token_exp         ON " + ident(m.heartbeatTable) + " TYPE option<datetime>;",
		}
		for _, s := range stmts {
			if _, err := surrealdb.Query[any](m.ctx, conn, s, nil); err != nil {
				m.logger.Warn("surrealdb: heartbeat schema statement rejected (user override is fine)",
					zap.String("table", m.heartbeatTable), zap.String("stmt", s), zap.Error(err))
			}
		}
	})

	_, err := surrealdb.Query[any](m.ctx, conn,
		`UPSERT type::record($tb, $id) MERGE {
		status: "running",
		last_heartbeat_at: time::now(),
		started_at: (SELECT VALUE started_at FROM type::record($tb, $id))[0] ?? time::now(),
		endpoint: $endpoint,
		ns: $ns,
		db: $db,
		level: $level,
		token_exp: $token_exp
	}`,
		map[string]any{
			"tb":        m.heartbeatTable,
			"id":        m.instanceName,
			"endpoint":  m.endpoint,
			"ns":        m.ns,
			"db":        m.db,
			"level":     string(m.auth.Level),
			"token_exp": m.tokenExp,
		})
	if err != nil {
		return fmt.Errorf("heartbeat upsert: %w", err)
	}
	return nil
}

// ensureWSScheme rewrites http(s):// to ws(wss):// so live queries work.
func ensureWSScheme(endpoint string) string {
	switch {
	case strings.HasPrefix(endpoint, "http://"):
		return "ws://" + endpoint[len("http://"):]
	case strings.HasPrefix(endpoint, "https://"):
		return "wss://" + endpoint[len("https://"):]
	}
	return endpoint
}

// parseTokenExpiry extracts the `exp` claim from a JWT without verifying the
// signature (signature validity is the server's concern; we only need the
// expiry to time proactive refresh).
func parseTokenExpiry(token string, logger *zap.Logger) time.Time {
	if token == "" {
		return time.Time{}
	}
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return time.Time{}
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		if logger != nil {
			logger.Debug("surrealdb: failed to decode JWT payload", zap.Error(err))
		}
		return time.Time{}
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}
	}
	switch v := claims["exp"].(type) {
	case float64:
		return time.Unix(int64(v), 0)
	case json.Number:
		n, _ := v.Int64()
		return time.Unix(n, 0)
	case string:
		var n int64
		fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			return time.Unix(n, 0)
		}
	}
	return time.Time{}
}

// ident quotes a SurrealQL identifier if it contains anything other than
// [A-Za-z0-9_]. Used for safe interpolation of user-provided table names.
func ident(name string) string {
	for _, r := range name {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return fmt.Sprintf("⟨%s⟩", name)
	}
	return name
}
