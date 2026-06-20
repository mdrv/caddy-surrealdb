package caddysurrealdb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

// parseCaddyfile is the httpcaddyfile directive helper. It returns the
// configured middleware handler.
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	m := new(Middleware)
	if err := m.UnmarshalCaddyfile(h.Dispenser); err != nil {
		return nil, err
	}
	return m, nil
}

// UnmarshalCaddyfile populates Middleware from the Caddyfile.
//
// Syntax:
//
//		surrealdb {
//		endpoint ws://127.0.0.1:596
//		namespace caddy
//		database caddy
//		auth root <user> <pass>            # or: namespace <u> <p> | database <u> <p>
//		                                 #     record <access> <bearer-key> | token <jwt>
//		schema {
//			<SurrealQL statements separated by ;>
//		}
//		query <name> {
//			sql `...`            # or sql_file <path>
//			param <n> { from body|query|header|path|env|placeholder; key <k>; type <t>; default <v> }
//			output { format json|csv|ndjson; envelope on|off; status <n>; body ""; omit <col>; alias <col> <name> }
//			cache 30s; timeout 10s
//		}
//		route <METHOD> <path> <query> {
//			require_header <name> <val>
//			rate_limit <n> per <dur>
//		}
//		live_query <name> {
//			table <t>; diff on|off; format sse|ws
//		}
//		live_route <METHOD> <path> <live_query_name>
//		log {
//			table <t>; batch_size <n>; flush_interval <dur>; buffer_size <n>
//			overflow drop|block; exclude_path <path>; omit <field>
//		}
//		heartbeat <dur>                   # ping interval
//		heartbeat_table <t>               # opt-in; default keeps user DB clean
//		token_refresh <dur>               # re-sign-in threshold before JWT exp
//		api_path <path>; api_token <token>; raw_sql on|off
//		cors_origin <origin>
//		}
func (m *Middleware) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()
	for d.NextBlock(0) {
		key := d.Val()
		switch key {
		case "endpoint":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.Endpoint = d.Val()

		case "namespace":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.Namespace = d.Val()

		case "database":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.Database = d.Val()

		case "auth":
			if err := m.parseAuth(d); err != nil {
				return err
			}

		case "schema":
			if err := m.parseSchema(d); err != nil {
				return err
			}

		case "query":
			if err := m.parseQuery(d); err != nil {
				return err
			}

		case "route":
			if err := m.parseRoute(d); err != nil {
				return err
			}

		case "live_query":
			if err := m.parseLiveQuery(d); err != nil {
				return err
			}

		case "live_route":
			if err := m.parseLiveRoute(d); err != nil {
				return err
			}

		case "log":
			if err := m.parseLog(d); err != nil {
				return err
			}

		case "heartbeat":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return d.WrapErr(fmt.Errorf("heartbeat: %w", err))
			}
			m.HeartbeatInterval = dur

		case "heartbeat_table":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.HeartbeatTable = d.Val()

		case "token_refresh":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return d.WrapErr(fmt.Errorf("token_refresh: %w", err))
			}
			m.TokenRefreshThreshold = dur

		case "api_path":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.APIPath = d.Val()

		case "api_token":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.APIToken = d.Val()

		case "raw_sql":
			if d.NextArg() {
				m.RawSQL = parseBool(d.Val())
			} else {
				m.RawSQL = true
			}

		case "cors_origin":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.CORSOrigin = d.Val()

		case "max_rows":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.WrapErr(err)
			}
			m.MaxRows = n

		default:
			return d.Errf("unknown directive %q", key)
		}
	}
	return nil
}

func (m *Middleware) parseAuth(d *caddyfile.Dispenser) error {
	if !d.NextArg() {
		// block form
		for nesting := d.Nesting(); d.NextBlock(nesting); {
			level := AuthLevel(d.Val())
			switch level {
			case AuthRoot, AuthNamespace, AuthDatabase:
				if !d.NextArg() {
					return d.ArgErr()
				}
				user := d.Val()
				if !d.NextArg() {
					return d.ArgErr()
				}
				pass := d.Val()
				m.Auth = AuthConfig{Level: level, Username: user, Password: pass}
			case AuthRecord:
				if !d.NextArg() {
					return d.ArgErr()
				}
				access := d.Val()
				if !d.NextArg() {
					return d.ArgErr()
				}
				key := d.Val()
				m.Auth = AuthConfig{Level: level, Access: access, BearerKey: key}
			case AuthToken:
				if !d.NextArg() {
					return d.ArgErr()
				}
				m.Auth = AuthConfig{Level: level, Token: d.Val()}
			default:
				return d.Errf("unknown auth level %q (want root|namespace|database|record|token)", d.Val())
			}
		}
		return nil
	}
	// inline form: auth <level> [args...]
	level := AuthLevel(d.Val())
	switch level {
	case AuthRoot, AuthNamespace, AuthDatabase:
		if !d.NextArg() {
			return d.ArgErr()
		}
		user := d.Val()
		if !d.NextArg() {
			return d.ArgErr()
		}
		m.Auth = AuthConfig{Level: level, Username: user, Password: d.Val()}
	case AuthRecord:
		if !d.NextArg() {
			return d.ArgErr()
		}
		access := d.Val()
		if !d.NextArg() {
			return d.ArgErr()
		}
		m.Auth = AuthConfig{Level: level, Access: access, BearerKey: d.Val()}
	case AuthToken:
		if !d.NextArg() {
			return d.ArgErr()
		}
		m.Auth = AuthConfig{Level: level, Token: d.Val()}
	default:
		return d.Errf("unknown auth level %q", level)
	}
	return nil
}

func (m *Middleware) parseSchema(d *caddyfile.Dispenser) error {
	// Block body is raw SurrealQL; multiple statements separated by ;
	var sb strings.Builder
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		if sb.Len() > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(d.Val())
		for d.NextArg() {
			sb.WriteByte(' ')
			sb.WriteString(d.Val())
		}
	}
	if sb.Len() == 0 {
		return nil
	}
	for _, stmt := range splitSurrealQL(sb.String()) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		m.Schema = append(m.Schema, stmt)
	}
	return nil
}

func (m *Middleware) parseQuery(d *caddyfile.Dispenser) error {
	if !d.NextArg() {
		return d.ArgErr()
	}
	name := d.Val()
	q := QueryDef{Name: name}
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "sql":
			if !d.NextArg() {
				return d.ArgErr()
			}
			q.SQL = d.Val()
		case "sql_file":
			if !d.NextArg() {
				return d.ArgErr()
			}
			q.SQLFile = d.Val()
		case "param":
			p, err := parseParam(d)
			if err != nil {
				return err
			}
			q.Params = append(q.Params, p)
		case "output":
			if err := parseOutput(d, &q.Output); err != nil {
				return err
			}
		case "cache":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return d.WrapErr(err)
			}
			q.CacheTTL = CacheDuration(dur)
		case "timeout":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return d.WrapErr(err)
			}
			q.Timeout = dur
		default:
			return d.Errf("unknown query sub-directive %q", d.Val())
		}
	}
	m.Queries = append(m.Queries, q)
	return nil
}

func parseParam(d *caddyfile.Dispenser) (ParamBinding, error) {
	if !d.NextArg() {
		return ParamBinding{}, d.ArgErr()
	}
	p := ParamBinding{Name: d.Val()}
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "from", "source":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			p.Source = d.Val()
		case "key":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			p.Key = d.Val()
		case "type":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			p.Type = d.Val()
		case "default":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			p.Default = d.Val()
		case "min":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			f, err := strconv.ParseFloat(d.Val(), 64)
			if err != nil {
				return p, d.WrapErr(err)
			}
			p.Min = &f
		case "max":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			f, err := strconv.ParseFloat(d.Val(), 64)
			if err != nil {
				return p, d.WrapErr(err)
			}
			p.Max = &f
		case "cap":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			f, err := strconv.ParseFloat(d.Val(), 64)
			if err != nil {
				return p, d.WrapErr(err)
			}
			p.Cap = &f
		case "pattern":
			if !d.NextArg() {
				return p, d.ArgErr()
			}
			p.Pattern = d.Val()
		default:
			return p, d.Errf("unknown param sub-directive %q", d.Val())
		}
	}
	return p, nil
}

func parseOutput(d *caddyfile.Dispenser, out *OutputConfig) error {
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "format":
			if !d.NextArg() {
				return d.ArgErr()
			}
			out.Format = d.Val()
		case "envelope":
			if d.NextArg() {
				out.Envelope = parseBool(d.Val())
			} else {
				out.Envelope = true
			}
		case "status":
			if !d.NextArg() {
				return d.ArgErr()
			}
			out.Status = statusFromString(d.Val())
		case "body":
			if !d.NextArg() {
				return d.ArgErr()
			}
			out.Body = d.Val()
		case "omit":
			for d.NextArg() {
				out.Omit = append(out.Omit, d.Val())
			}
		case "alias":
			if !d.NextArg() {
				return d.ArgErr()
			}
			from := d.Val()
			if !d.NextArg() {
				return d.ArgErr()
			}
			to := d.Val()
			if out.Aliases == nil {
				out.Aliases = make(map[string]string)
			}
			out.Aliases[from] = to
		default:
			return d.Errf("unknown output sub-directive %q", d.Val())
		}
	}
	return nil
}

func (m *Middleware) parseRoute(d *caddyfile.Dispenser) error {
	var method, path, query string
	if !d.Args(&method, &path, &query) {
		return d.ArgErr()
	}
	rt := QueryRoute{Method: strings.ToUpper(method), Path: path, QueryName: query}
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "require_header":
			if !d.NextArg() {
				return d.ArgErr()
			}
			name := d.Val()
			val := ""
			if d.NextArg() {
				val = d.Val()
			}
			if rt.RequireHeaders == nil {
				rt.RequireHeaders = make(map[string]string)
			}
			rt.RequireHeaders[name] = val
		case "rate_limit":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.WrapErr(err)
			}
			if !d.NextArg() || d.Val() != "per" {
				return d.Err("expected `per <duration>`")
			}
			if !d.NextArg() {
				return d.ArgErr()
			}
			window, err := time.ParseDuration(d.Val())
			if err != nil {
				return d.WrapErr(err)
			}
			rt.RateLimit = &RateLimit{Requests: n, Window: window}
		default:
			return d.Errf("unknown route sub-directive %q", d.Val())
		}
	}
	m.Routes = append(m.Routes, rt)
	return nil
}

func (m *Middleware) parseLiveQuery(d *caddyfile.Dispenser) error {
	if !d.NextArg() {
		return d.ArgErr()
	}
	lq := LiveQueryDef{Name: d.Val()}
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "table":
			if !d.NextArg() {
				return d.ArgErr()
			}
			lq.Table = d.Val()
		case "where":
			if !d.NextArg() {
				return d.ArgErr()
			}
			lq.Where = d.Val()
		case "diff":
			if d.NextArg() {
				lq.Diff = parseBool(d.Val())
			} else {
				lq.Diff = true
			}
		case "format":
			if !d.NextArg() {
				return d.ArgErr()
			}
			lq.Format = LiveFormat(d.Val())
		default:
			return d.Errf("unknown live_query sub-directive %q", d.Val())
		}
	}
	m.LiveQueries = append(m.LiveQueries, lq)
	return nil
}

func (m *Middleware) parseLiveRoute(d *caddyfile.Dispenser) error {
	var method, path, query string
	if !d.Args(&method, &path, &query) {
		return d.ArgErr()
	}
	lr := LiveRoute{Method: strings.ToUpper(method), Path: path, QueryName: query}
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "require_header":
			if !d.NextArg() {
				return d.ArgErr()
			}
			name := d.Val()
			val := ""
			if d.NextArg() {
				val = d.Val()
			}
			if lr.RequireHeaders == nil {
				lr.RequireHeaders = make(map[string]string)
			}
			lr.RequireHeaders[name] = val
		default:
			return d.Errf("unknown live_route sub-directive %q", d.Val())
		}
	}
	m.LiveRoutes = append(m.LiveRoutes, lr)
	return nil
}

func (m *Middleware) parseLog(d *caddyfile.Dispenser) error {
	for nesting := d.Nesting(); d.NextBlock(nesting); {
		switch d.Val() {
		case "table":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.LogTable = d.Val()
		case "batch_size":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.WrapErr(err)
			}
			m.BatchSize = n
		case "flush_interval":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := time.ParseDuration(d.Val())
			if err != nil {
				return d.WrapErr(err)
			}
			m.FlushInterval = dur
		case "buffer_size":
			if !d.NextArg() {
				return d.ArgErr()
			}
			n, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.WrapErr(err)
			}
			m.BufferSize = n
		case "overflow":
			if !d.NextArg() {
				return d.ArgErr()
			}
			m.Overflow = d.Val()
		case "exclude_path":
			for d.NextArg() {
				m.LogExcludePaths = append(m.LogExcludePaths, d.Val())
			}
		case "omit":
			for d.NextArg() {
				m.LogExcludeFields = append(m.LogExcludeFields, d.Val())
			}
		default:
			return d.Errf("unknown log sub-directive %q", d.Val())
		}
	}
	return nil
}

// parseBool accepts on/off/true/false/yes/no/1/0.
func parseBool(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "on", "true", "yes", "1":
		return true
	default:
		return false
	}
}

// splitSurrealQL tokenizes a multi-statement string on semicolons that are
// NOT inside quotes, backticks, or parentheses.
func splitSurrealQL(s string) []string {
	var out []string
	depth := 0
	var cur strings.Builder
	var quoteCh byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quoteCh != 0:
			cur.WriteByte(c)
			if c == quoteCh && (i == 0 || s[i-1] != '\\') {
				quoteCh = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quoteCh = c
			cur.WriteByte(c)
		case c == '(':
			depth++
			cur.WriteByte(c)
		case c == ')':
			if depth > 0 {
				depth--
			}
			cur.WriteByte(c)
		case c == ';' && depth == 0:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

// parseCaddyfile is the httpcaddyfile registration helper.
//
// (Defined above as a function returning []httpcaddyfile.ConfigValue; this
// stub is unused — kept for documentation.)
var _ caddy.Module = (*Middleware)(nil)
var _ caddyhttp.MiddlewareHandler = (*Middleware)(nil)
var _ caddyfile.Unmarshaler = (*Middleware)(nil)
