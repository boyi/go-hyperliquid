package hyperliquid

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestNewWebsocketClient(t *testing.T) {
	require.PanicsWithValue(t,
		"baseURL must have a scheme set, either wss or ws",
		func() { _ = NewWebsocketClient("foobar.com") },
	)

	require.NotPanics(t,
		func() { _ = NewWebsocketClient(MainnetAPIURL) },
		"Mainnet should always work",
	)

	require.NotPanics(t,
		func() { _ = NewWebsocketClient("") },
		"empty URL should default to Mainnet",
	)
}

func TestWsOptReadTimeout(t *testing.T) {
	client := NewWebsocketClient(MainnetAPIURL, WsOptReadTimeout(42*time.Second))
	require.Equal(t, 42*time.Second, client.readTimeout)
}

// TestReadPumpReconnectsOnTimeout spins up a WebSocket server that accepts
// connections but never sends a message.  The client should time out and
// reconnect, resulting in more than one TCP-level upgrade.
func TestReadPumpReconnectsOnTimeout(t *testing.T) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	var connectCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connectCount.Add(1)
		// Hold the connection open; drain any frames the client sends (e.g. ping)
		// so the TCP link itself stays alive — only application-layer data is absent.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				_ = conn.Close()
				return
			}
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 200 ms timeout gives us several reconnect cycles within the 3 s context.
	client := NewWebsocketClient(server.URL, WsOptReadTimeout(200*time.Millisecond))
	require.NoError(t, client.Connect(ctx))

	// Allow enough wall-clock time for multiple timeout → reconnect cycles.
	time.Sleep(2 * time.Second)

	require.GreaterOrEqual(t, int(connectCount.Load()), 2,
		"client should have reconnected at least once after read timeout")

	require.NoError(t, client.Close())
}

// logRecorder collects WsOptLogf output for assertions.
type logRecorder struct {
	mu    sync.Mutex
	lines []string
}

func (r *logRecorder) logf(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, fmt.Sprintf(format, args...))
}

func (r *logRecorder) has(substr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, l := range r.lines {
		if strings.Contains(l, substr) {
			return true
		}
	}
	return false
}

func (r *logRecorder) dump() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.lines, "\n")
}

// TestWsOptLogfLifecycle checks that, without debug mode, WsOptLogf sees the
// subscription ack, a server error message, a server-side disconnect, and the
// ping-driven reconnect with its resubscribe, and never sees market data.
func TestWsOptLogfLifecycle(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connectCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		n := connectCount.Add(1)
		defer conn.Close()
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var cmd struct {
				Method string `json:"method"`
			}
			_ = json.Unmarshal(msg, &cmd)
			if cmd.Method != "subscribe" {
				continue
			}
			_ = conn.WriteMessage(websocket.TextMessage, []byte(
				`{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"bbo","coin":"PONS"}}}`))
			_ = conn.WriteMessage(websocket.TextMessage, []byte(
				`{"channel":"bbo","data":{"coin":"PONS","time":1,"bbo":[{"px":"0.65","sz":"1","n":1},{"px":"0.66","sz":"1","n":1}]}}`))
			_ = conn.WriteMessage(websocket.TextMessage, []byte(
				`{"channel":"error","data":"Invalid subscription test"}`))
			if n == 1 {
				// Drop the first connection right after serving it.
				return
			}
		}
	}))
	defer server.Close()

	rec := &logRecorder{}
	client := NewWebsocketClient(server.URL, WsOptLogf(rec.logf))
	require.NoError(t, client.Connect(context.Background()))
	defer client.Close()

	var bboCount atomic.Int32
	_, err := client.Bbo(BboSubscriptionParams{Coin: "PONS"}, func(Bbo, error) { bboCount.Add(1) })
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return rec.has("subscription response:") &&
			rec.has("server error message: \"Invalid subscription test\"") &&
			rec.has("websocket read error")
	}, 3*time.Second, 20*time.Millisecond, rec.dump())
	require.True(t, rec.has("websocket connected url="), rec.dump())
	require.False(t, rec.has("no dispatcher"), rec.dump())
	require.False(t, rec.has(`"px"`), "market data must not be logged:\n"+rec.dump())
	require.GreaterOrEqual(t, bboCount.Load(), int32(1))

	// Reconnect is driven by the ping pump; trigger it directly instead of
	// waiting for pingInterval.
	require.False(t, client.pingOnce(context.Background()), "ping on a dropped connection must fail and reconnect")
	require.Eventually(t, func() bool {
		return rec.has("websocket reconnect attempt 1 succeeded") &&
			rec.has("resubscribed 1/1 subscriptions")
	}, 3*time.Second, 20*time.Millisecond, rec.dump())
	require.GreaterOrEqual(t, connectCount.Load(), int32(2))
}

// TestWsNoLoggerIsSilent guards the default: without any logging option the
// new dispatchers must not panic or error.
func TestWsNoLoggerIsSilent(t *testing.T) {
	client := NewWebsocketClient(MainnetAPIURL)
	for _, ch := range []string{ChannelSubResponse, ChannelError} {
		require.NoError(t, client.dispatch(wsMessage{Channel: ch, Data: json.RawMessage(`"x"`)}))
	}
}
