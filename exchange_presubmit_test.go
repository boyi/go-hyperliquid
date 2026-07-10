package hyperliquid

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

type presubmitCtxKey struct{}

// ctxProbeTransport records the value PreSubmitFunc stashed on the context, as
// observed on the outgoing request — proof the derived context survives signing.
type ctxProbeTransport struct {
	inner http.RoundTripper
	mu    sync.Mutex
	seen  []any
}

func (t *ctxProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.seen = append(t.seen, r.Context().Value(presubmitCtxKey{}))
	t.mu.Unlock()
	return t.inner.RoundTrip(r)
}

func (t *ctxProbeTransport) observed() []any {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]any(nil), t.seen...)
}

func newPreSubmitExchange(t *testing.T, probe *ctxProbeTransport, opts ...ExchangeOpt) *Exchange {
	t.Helper()
	priv, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok","response":{"type":"cancel","data":{"statuses":["success"]}}}`))
	}))
	t.Cleanup(srv.Close)

	base := append([]ExchangeOpt{
		ExchangeOptClientOptions(ClientOptHTTPClient(&http.Client{Transport: probe})),
	}, opts...)
	meta := &Meta{Universe: []AssetInfo{{Name: "BTC", SzDecimals: 5}}}
	return NewExchange(context.Background(), priv, srv.URL, meta, "", "", &SpotMeta{}, nil, base...)
}

// TestPreSubmitRunsBeforeNonce is the whole point of the hook: whatever waiting a
// caller does — a rate-limit token, admission control — must happen on the near
// side of the nonce, or the nonce ages during the wait and Hyperliquid rejects it
// as "nonce too low". It also pins that the context the hook returns reaches the
// HTTP request, so a caller can hand a reservation downstream.
func TestPreSubmitRunsBeforeNonce(t *testing.T) {
	var mu sync.Mutex
	var order []string
	record := func(s string) {
		mu.Lock()
		order = append(order, s)
		mu.Unlock()
	}

	probe := &ctxProbeTransport{inner: http.DefaultTransport}
	ex := newPreSubmitExchange(t, probe,
		ExchangeOptNonceFunc(func() int64 {
			record("nonce")
			return time.Now().UnixMilli()
		}),
		ExchangeOptPreSubmit(func(ctx context.Context) (context.Context, error) {
			record("preSubmit")
			return context.WithValue(ctx, presubmitCtxKey{}, "reserved"), nil
		}),
	)

	if _, err := ex.Cancel(context.Background(), "BTC", 1); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if len(got) != 2 || got[0] != "preSubmit" || got[1] != "nonce" {
		t.Fatalf("hook must run before nonce allocation; order=%v", got)
	}
	if seen := probe.observed(); len(seen) != 1 || seen[0] != "reserved" {
		t.Fatalf("context from PreSubmitFunc did not reach the request; seen=%v", seen)
	}
}

// TestPreSubmitErrorAbortsBeforeNonce: a hook that fails (context cancelled while
// waiting for a token) must burn neither a nonce nor a request.
func TestPreSubmitErrorAbortsBeforeNonce(t *testing.T) {
	nonces := 0
	probe := &ctxProbeTransport{inner: http.DefaultTransport}
	ex := newPreSubmitExchange(t, probe,
		ExchangeOptNonceFunc(func() int64 {
			nonces++
			return time.Now().UnixMilli()
		}),
		ExchangeOptPreSubmit(func(ctx context.Context) (context.Context, error) {
			return ctx, context.Canceled
		}),
	)

	if _, err := ex.Cancel(context.Background(), "BTC", 1); err != context.Canceled {
		t.Fatalf("cancel err = %v, want context.Canceled", err)
	}
	if nonces != 0 {
		t.Fatalf("aborted submit consumed %d nonce(s)", nonces)
	}
	if seen := probe.observed(); len(seen) != 0 {
		t.Fatalf("aborted submit still sent %d request(s)", len(seen))
	}
}

// TestPreSubmitAbsentIsUnchanged guards the default: no hook, no behaviour change.
func TestPreSubmitAbsentIsUnchanged(t *testing.T) {
	probe := &ctxProbeTransport{inner: http.DefaultTransport}
	ex := newPreSubmitExchange(t, probe)

	if _, err := ex.Cancel(context.Background(), "BTC", 1); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if seen := probe.observed(); len(seen) != 1 || seen[0] != nil {
		t.Fatalf("unexpected context value without a hook; seen=%v", seen)
	}
}
