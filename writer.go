package caddysurrealdb

import (
	"context"
	"sync"
	"time"

	surrealdb "github.com/surrealdb/surrealdb.go"
	"go.uber.org/zap"
)

// RequestRecord captures one HTTP transaction for batched logging to SurrealDB.
type RequestRecord struct {
	TS        time.Time
	IP        string
	Method    string
	Host      string
	Path      string
	Query     string
	Status    int
	LatencyMs int64
	BytesSent int64
	UserAgent string
}

// BatchWriter fans request records into a SurrealDB table in batches. It is
// channel-fed and non-blocking when Overflow == "drop"; "block" back-pressures
// the caller.
type BatchWriter struct {
	mgr     *Manager
	table   string
	exclude []string
	omit    []string

	batchSize     int
	flushInterval time.Duration
	bufferSize    int
	overflow      string // drop|block

	ch         chan RequestRecord
	stopCh     chan struct{}
	wg         sync.WaitGroup
	logger     *zap.Logger
	schemaOnce sync.Once

	mu  sync.Mutex
	buf []RequestRecord
}

// BatchWriterConfig configures a new writer.
type BatchWriterConfig struct {
	Table         string
	ExcludePaths  []string
	OmitFields    []string
	BatchSize     int
	FlushInterval time.Duration
	BufferSize    int
	Overflow      string
}

// NewBatchWriter constructs and starts a writer.
func NewBatchWriter(mgr *Manager, cfg BatchWriterConfig, logger *zap.Logger) *BatchWriter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 200 * time.Millisecond
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 8192
	}
	if cfg.Overflow == "" {
		cfg.Overflow = "drop"
	}
	if cfg.Table == "" {
		cfg.Table = "_caddy_requests"
	}
	w := &BatchWriter{
		mgr:           mgr,
		table:         cfg.Table,
		exclude:       cfg.ExcludePaths,
		omit:          cfg.OmitFields,
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		bufferSize:    cfg.BufferSize,
		overflow:      overflowMode(cfg.Overflow),
		ch:            make(chan RequestRecord, cfg.BufferSize),
		stopCh:        make(chan struct{}),
		logger:        logger,
		buf:           make([]RequestRecord, 0, cfg.BatchSize),
	}
	w.wg.Add(1)
	go w.run()
	return w
}

// overflowMode coerces the config string to a valid overflow mode.
func overflowMode(s string) string {
	if s == "block" {
		return "block"
	}
	return "drop"
}

// Write enqueues a record. Returns false if the record was dropped (overflow=drop).
func (w *BatchWriter) Write(r RequestRecord) bool {
	if w == nil || w.table == "" {
		return false
	}
	for _, ex := range w.exclude {
		if r.Path == ex {
			return true
		}
	}
	if w.overflow == "drop" {
		select {
		case w.ch <- r:
			return true
		default:
			return false
		}
	}
	w.ch <- r
	return true
}

// Close drains and stops the writer.
func (w *BatchWriter) Close() {
	close(w.stopCh)
	w.wg.Wait()
}

func (w *BatchWriter) run() {
	defer w.wg.Done()
	tick := time.NewTicker(w.flushInterval)
	defer tick.Stop()

	for {
		select {
		case <-w.stopCh:
			w.drain()
			w.flush()
			return
		case r := <-w.ch:
			w.mu.Lock()
			w.buf = append(w.buf, r)
			full := len(w.buf) >= w.batchSize
			w.mu.Unlock()
			if full {
				w.flush()
			}
		case <-tick.C:
			w.flush()
		}
	}
}

// drain pulls anything still in the channel into the buffer at shutdown.
func (w *BatchWriter) drain() {
	for {
		select {
		case r := <-w.ch:
			w.mu.Lock()
			w.buf = append(w.buf, r)
			w.mu.Unlock()
		default:
			return
		}
	}
}

func (w *BatchWriter) flush() {
	w.mu.Lock()
	if len(w.buf) == 0 {
		w.mu.Unlock()
		return
	}
	batch := w.buf
	w.buf = make([]RequestRecord, 0, w.batchSize)
	w.mu.Unlock()

	conn, err := w.mgr.Get(context.Background())
	if err != nil {
		w.logger.Warn("surrealdb: writer flush skipped (not connected)",
			zap.Int("records", len(batch)), zap.Error(err))
		return
	}

	// Lazily define the log table the first time we successfully connect.
	// We use SCHEMALESS + typed FIELDs: SurrealDB enforces types on declared
	// fields but still tolerates extra fields, so future struct additions
	// don't reject old rows. DEFINE TABLE/FIELD IF NOT EXISTS won't override
	// a user's own definition from the Schema block.
	w.schemaOnce.Do(func() {
		stmts := []string{
			"DEFINE TABLE IF NOT EXISTS " + ident(w.table) + " SCHEMALESS PERMISSIONS FOR select, create WHERE true;",
			"DEFINE FIELD IF NOT EXISTS ts         ON " + ident(w.table) + " TYPE datetime;",
			"DEFINE FIELD IF NOT EXISTS ip         ON " + ident(w.table) + " TYPE string;",
			"DEFINE FIELD IF NOT EXISTS method     ON " + ident(w.table) + " TYPE string;",
			"DEFINE FIELD IF NOT EXISTS host       ON " + ident(w.table) + " TYPE string;",
			"DEFINE FIELD IF NOT EXISTS path       ON " + ident(w.table) + " TYPE string;",
			"DEFINE FIELD IF NOT EXISTS query      ON " + ident(w.table) + " TYPE option<string>;",
			"DEFINE FIELD IF NOT EXISTS status     ON " + ident(w.table) + " TYPE int;",
			"DEFINE FIELD IF NOT EXISTS latency_ms ON " + ident(w.table) + " TYPE int;",
			"DEFINE FIELD IF NOT EXISTS bytes_sent ON " + ident(w.table) + " TYPE int;",
			"DEFINE FIELD IF NOT EXISTS user_agent ON " + ident(w.table) + " TYPE option<string>;",
			"DEFINE INDEX IF NOT EXISTS idx_" + ident(w.table) + "_ts     ON " + ident(w.table) + " FIELDS ts;",
			"DEFINE INDEX IF NOT EXISTS idx_" + ident(w.table) + "_path   ON " + ident(w.table) + " FIELDS path;",
			"DEFINE INDEX IF NOT EXISTS idx_" + ident(w.table) + "_status ON " + ident(w.table) + " FIELDS status;",
		}
		for _, s := range stmts {
			if _, err := surrealdb.Query[any](context.Background(), conn, s, nil); err != nil {
				w.logger.Warn("surrealdb: log table schema statement rejected (user override is fine)",
					zap.String("table", w.table), zap.String("stmt", s), zap.Error(err))
			}
		}
	})

	// Build records parametrically so SurrealDB does the type coercion.
	rows := make([]map[string]any, 0, len(batch))
	for _, r := range batch {
		row := map[string]any{
			"ts":         r.TS,
			"ip":         r.IP,
			"method":     r.Method,
			"host":       r.Host,
			"path":       r.Path,
			"query":      r.Query,
			"status":     r.Status,
			"latency_ms": r.LatencyMs,
			"bytes_sent": r.BytesSent,
			"user_agent": r.UserAgent,
		}
		for _, f := range w.omit {
			delete(row, f)
		}
		rows = append(rows, row)
	}

	// INSERT IGNORE-style: use INSERT ( SurrealDB has no ON CONFLICT ) — these
	// are append-only request logs, so plain INSERT is correct.
	sql := "INSERT INTO " + ident(w.table) + " $rows;"
	if _, err := surrealdb.Query[any](context.Background(), conn, sql,
		map[string]any{"rows": rows}); err != nil {
		w.logger.Warn("surrealdb: writer flush failed",
			zap.Int("records", len(rows)), zap.Error(err))
	}
}
