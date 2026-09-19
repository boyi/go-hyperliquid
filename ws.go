package hyperliquid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sonirico/vago/lol"
	"github.com/sonirico/vago/maps"
)

const (
	// pingInterval is the interval for sending ping messages to keep WebSocket alive
	pingInterval = 50 * time.Second

	// wsReadTimeout is the default maximum duration to wait for a single read from
	// the server before treating the connection as stalled. Must exceed pingInterval
	// so that normal pong responses do not trigger a false timeout.
	wsReadTimeout = 90 * time.Second
)

type Subscription struct {
	ID      string
	Payload any
	Close   func()
}

type WebsocketClient struct {
	url  string
	conn *websocket.Conn
	// writeConn mirrors conn for writeJSON, which runs both with and without
	// mu held and therefore cannot take mu to read conn.
	writeConn             atomic.Pointer[websocket.Conn]
	dialer                *websocket.Dialer
	mu                    sync.RWMutex
	writeMu               sync.Mutex
	subscribers           map[string]*uniqSubscriber
	msgDispatcherRegistry map[string]msgDispatcher
	nextSubID             atomic.Int64
	done                  chan struct{}
	closeOnce             sync.Once
	reconnectWait         time.Duration
	readTimeout           time.Duration
	debug                 bool
	logger                lol.Logger
	// logf receives connection-lifecycle events and errors (see WsOptLogf).
	// Unlike debug mode it never sees per-message market data.
	logf func(format string, args ...any)
	// reconnectAttempt counts consecutive failed reconnects, for logging only.
	reconnectAttempt atomic.Int64
	// connGeneration counts established connections, so callers can detect that
	// the connection underneath them was replaced. See ConnectionGeneration.
	connGeneration atomic.Uint64
}

var upstreamHosts map[string]struct{}

func init() {
	mustHost := func(s string) string {
		u, err := url.Parse(s)
		if err != nil {
			panic(fmt.Sprintf("invalid upstream URL %q: %v", s, err))
		}
		return strings.ToLower(u.Hostname())
	}
	upstreamHosts = map[string]struct{}{
		mustHost(MainnetAPIURL): {},
		mustHost(TestnetAPIURL): {},
	}
}

func isUpstream(u *url.URL) bool {
	_, ok := upstreamHosts[strings.ToLower(u.Hostname())]
	return ok
}

func NewWebsocketClient(baseURL string, opts ...WsOpt) *WebsocketClient {
	if baseURL == "" {
		baseURL = MainnetAPIURL
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		log.Fatalf("invalid URL: %v", err)
	}

	// the current usage expects a full address (https://api.hyp..) to keep compatibility check if
	// that host is set and just use the old method. any new caller with their own endpoint will be
	// forced to provide a full URI
	if isUpstream(parsedURL) {
		parsedURL.Scheme = "wss"
		parsedURL.Path = "/ws"
	} else {
		switch parsedURL.Scheme {
		case "https":
			parsedURL.Scheme = "wss"
		case "http":
			parsedURL.Scheme = "ws"
		case "":
			// baseURL has no scheme set, odd
			panic("baseURL must have a scheme set, either wss or ws")
		}
	}

	wsURL := parsedURL.String()

	cli := &WebsocketClient{
		url:           wsURL,
		done:          make(chan struct{}),
		reconnectWait: time.Second,
		readTimeout:   wsReadTimeout,
		subscribers:   make(map[string]*uniqSubscriber),
		msgDispatcherRegistry: map[string]msgDispatcher{
			ChannelPong:           NewPongDispatcher(),
			ChannelTrades:         NewMsgDispatcher[Trades](ChannelTrades),
			ChannelActiveAssetCtx: NewMsgDispatcher[ActiveAssetCtx](ChannelActiveAssetCtx),
			ChannelL2Book:         NewMsgDispatcher[L2Book](ChannelL2Book),
			ChannelCandle:         NewMsgDispatcher[Candle](ChannelCandle),
			ChannelAllMids:        NewMsgDispatcher[AllMids](ChannelAllMids),
			ChannelNotification:   NewMsgDispatcher[Notification](ChannelNotification),
			ChannelOrderUpdates:   NewMsgDispatcher[WsOrders](ChannelOrderUpdates),
			ChannelWebData2:       NewMsgDispatcher[WebData2](ChannelWebData2),
			ChannelBbo:            NewMsgDispatcher[Bbo](ChannelBbo),
			ChannelUserFills:      NewMsgDispatcher[WsOrderFills](ChannelUserFills),
			ChannelSubResponse:    NewNoopDispatcher(),
			ChannelClearinghouseState: NewMsgDispatcher[ClearinghouseStateMessage](
				ChannelClearinghouseState,
			),
			ChannelOpenOrders: NewMsgDispatcher[OpenOrders](ChannelOpenOrders),
			ChannelTwapStates: NewMsgDispatcher[TwapStates](ChannelTwapStates),
			ChannelWebData3:   NewMsgDispatcher[WebData3](ChannelWebData3),
		},
	}

	// Subscription acks and server-side errors are rare, so they are logged
	// verbatim: they are the only way to tell "connection alive but this
	// subscription was rejected/dropped" apart from a dead connection.
	cli.msgDispatcherRegistry[ChannelSubResponse] = msgDispatcherFunc[any](
		func(_ []*uniqSubscriber, msg wsMessage) error {
			cli.logInfof("subscription response: %s", string(msg.Data))
			return nil
		},
	)
	cli.msgDispatcherRegistry[ChannelError] = msgDispatcherFunc[any](
		func(_ []*uniqSubscriber, msg wsMessage) error {
			cli.logErrf("server error message: %s", string(msg.Data))
			return nil
		},
	)

	for _, opt := range opts {
		opt.Apply(cli)
	}

	return cli
}

func (w *WebsocketClient) Connect(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		return nil
	}

	if w.dialer == nil {
		w.dialer = websocket.DefaultDialer
	}

	//nolint:bodyclose // WebSocket connections don't have response bodies to close
	conn, _, err := w.dialer.DialContext(ctx, w.url, nil)
	if err != nil {
		return fmt.Errorf("websocket dial: %w", err)
	}

	w.conn = conn
	w.writeConn.Store(conn)
	generation := w.connGeneration.Add(1)
	desc := connDesc(w.url, conn)
	w.logInfof("websocket connected %s subscriptions=%d generation=%d", desc, len(w.subscribers), generation)

	go w.readPump(ctx, desc)
	go w.pingPump(ctx)

	return w.resubscribeAll(desc)
}

// ConnectionGeneration reports how many connections this client has established:
// it starts at 0 and increments on every successful dial, the internal reconnects
// included. Subscribers are never told that a reconnect happened — readPump logs
// the failure and dials again underneath them — so recording this value and
// comparing it later is the only way a caller can tell whether the stream it is
// reasoning about ran on one unbroken connection. A caller that must not miss
// messages (an account stream feeding local state, say) can treat a changed
// generation as "resubscribed, so there may be a gap".
func (w *WebsocketClient) ConnectionGeneration() uint64 {
	return w.connGeneration.Load()
}

func connDesc(u string, conn *websocket.Conn) string {
	return fmt.Sprintf("url=%s local=%s remote=%s", u, conn.LocalAddr(), conn.RemoteAddr())
}

type Handler[T subscriptable] func(wsMessage) (T, error)

func (w *WebsocketClient) subscribe(
	payload subscriptable,
	callback func(any),
) (*Subscription, error) {
	if callback == nil {
		return nil, fmt.Errorf("callback cannot be nil")
	}

	w.mu.Lock()

	pkey := payload.Key()
	subscriber, exists := w.subscribers[pkey]
	if !exists {
		subscriber = newUniqSubscriber(
			pkey,
			payload,
			// on subscribe
			func(p subscriptable) {
				if err := w.sendSubscribe(p); err != nil {
					w.logErrf("failed to subscribe key=%s: %v", pkey, err)
				}
			},
			// on unsubscribe
			func(p subscriptable) {
				w.mu.Lock()
				defer w.mu.Unlock()
				delete(w.subscribers, pkey)
				if err := w.sendUnsubscribe(p); err != nil {
					w.logErrf("failed to unsubscribe key=%s: %v", pkey, err)
				}
			},
		)

		w.subscribers[pkey] = subscriber
	}

	w.mu.Unlock()

	nextID := w.nextSubID.Add(1)
	subID := key(pkey, strconv.Itoa(int(nextID)))
	subscriber.subscribe(subID, callback)

	return &Subscription{
		ID: subID,
		Close: func() {
			subscriber.unsubscribe(subID)
		},
	}, nil
}

func (w *WebsocketClient) Close() error {
	var err error
	w.closeOnce.Do(func() {
		err = w.close()
	})
	return err
}

func (w *WebsocketClient) close() error {
	close(w.done)
	w.logInfof("websocket client closed url=%s", w.url)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		return w.conn.Close()
	}

	for _, subscriber := range w.subscribers {
		subscriber.clear()
	}
	return nil
}

// Private methods

func (w *WebsocketClient) readPump(ctx context.Context, desc string) {
	shouldReconnect := false
	defer func() {
		w.mu.Lock()
		if w.conn != nil {
			_ = w.conn.Close() // Ignore close error in defer
			w.conn = nil
			w.writeConn.Store(nil)
		}
		w.mu.Unlock()

		if shouldReconnect {
			w.reconnect(ctx)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			w.logInfof("websocket read pump stopped (context done) %s", desc)
			return
		case <-w.done:
			w.logInfof("websocket read pump stopped (client closed) %s", desc)
			return
		default:
			if err := w.conn.SetReadDeadline(time.Now().Add(w.readTimeout)); err != nil {
				w.logErrf("websocket set read deadline %s: %v "+
					"(disconnected; ping pump will reconnect)", desc, err)
				return
			}

			_, msg, err := w.conn.ReadMessage()
			if err != nil {
				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() {
					w.logErrf("websocket read timeout after %s %s, reconnecting now",
						w.readTimeout, desc)
					shouldReconnect = true
					return
				}
				select {
				case <-w.done:
					w.logInfof("websocket read pump stopped (client closed) %s: %v", desc, err)
					return
				default:
				}
				// No immediate reconnect on this path: the connection is dropped
				// and the next ping (up to pingInterval later) triggers it.
				w.logErrf("websocket read error %s: %v "+
					"(disconnected; ping pump will reconnect within %s)", desc, err, pingInterval)
				return
			}

			if w.debug {
				w.logDebugf("[<] %s", string(msg))
			}

			var wsMsg wsMessage
			if err := json.Unmarshal(msg, &wsMsg); err != nil {
				w.logErrf("websocket message parse error: %v", err)
				continue
			}

			if err := w.dispatch(wsMsg); err != nil {
				w.logErrf("failed to dispatch websocket message: %v", err)
			}
		}
	}
}

func (w *WebsocketClient) pingPump(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !w.pingOnce(ctx) {
				return
			}
		}
	}
}

// pingOnce sends one ping. On failure it reconnects (which starts a new ping
// pump) and returns false so the caller's pump stops.
func (w *WebsocketClient) pingOnce(ctx context.Context) bool {
	if err := w.sendPing(); err != nil {
		w.logErrf("ping error url=%s: %v, reconnecting", w.url, err)
		w.reconnect(ctx)
		return false
	}
	return true
}

func (w *WebsocketClient) dispatch(msg wsMessage) error {
	dispatcher, ok := w.msgDispatcherRegistry[msg.Channel]
	if !ok {
		return fmt.Errorf("no dispatcher for channel: %s", msg.Channel)
	}

	w.mu.RLock()
	subscribers := maps.Values(w.subscribers)
	w.mu.RUnlock()

	return dispatcher.Dispatch(subscribers, msg)
}

func (w *WebsocketClient) reconnect(ctx context.Context) {
	for {
		select {
		case <-w.done:
			return
		case <-ctx.Done():
			return
		default:
			attempt := w.reconnectAttempt.Add(1)
			err := w.Connect(ctx)
			if err == nil {
				w.reconnectAttempt.Store(0)
				w.logInfof("websocket reconnect attempt %d succeeded url=%s", attempt, w.url)
				return
			}
			w.logErrf("websocket reconnect attempt %d failed url=%s: %v, retrying in %s",
				attempt, w.url, err, w.reconnectWait)
			time.Sleep(w.reconnectWait)
			w.reconnectWait *= 2 // TODO: configurable strategies such as exponential backoff and the like
			if w.reconnectWait > time.Minute {
				w.reconnectWait = time.Minute
			}
		}
	}
}

// resubscribeAll re-sends every active subscription on a fresh connection.
// It returns on the first failure, as before. The log line lists the keys
// that were never re-sent: a failure here leaves the connection up (a
// retried Connect is a no-op), so those subscriptions stay silent until the
// next reconnect.
func (w *WebsocketClient) resubscribeAll(desc string) error {
	total := len(w.subscribers)
	sent := make(map[string]struct{}, total)
	for key, subscriber := range w.subscribers {
		if err := w.sendSubscribe(subscriber.subscriptionPayload); err != nil {
			var missing []string
			for k := range w.subscribers {
				if _, ok := sent[k]; !ok {
					missing = append(missing, k)
				}
			}
			sort.Strings(missing)
			w.logErrf("resubscribe failed %s at key=%s after %d/%d sent: %v; not re-sent: %v",
				desc, key, len(sent), total, err, missing)
			return fmt.Errorf("resubscribe: %w", err)
		}
		sent[key] = struct{}{}
	}
	if total > 0 {
		w.logInfof("resubscribed %d/%d subscriptions %s", len(sent), total, desc)
	}
	return nil
}

func (w *WebsocketClient) sendSubscribe(payload subscriptable) error {
	return w.writeJSON(wsCommand{
		Method:       "subscribe",
		Subscription: payload,
	})
}

func (w *WebsocketClient) sendUnsubscribe(payload subscriptable) error {
	return w.writeJSON(wsCommand{
		Method:       "unsubscribe",
		Subscription: payload,
	})
}

func (w *WebsocketClient) sendPing() error {
	return w.writeJSON(wsCommand{Method: "ping"})
}

func (w *WebsocketClient) writeJSON(v any) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	conn := w.writeConn.Load()
	if conn == nil {
		return fmt.Errorf("connection closed")
	}

	if w.debug {
		bts, _ := json.Marshal(v)
		w.logDebugf("[>] %s", string(bts))
	}

	return conn.WriteJSON(v)
}

func (w *WebsocketClient) logErrf(format string, args ...any) {
	if w.logger != nil {
		w.logger.Errorf(format, args...)
	}
	if w.logf != nil {
		w.logf("[hl-ws][error] "+format, args...)
	}
}

func (w *WebsocketClient) logInfof(format string, args ...any) {
	if w.logger != nil {
		w.logger.Infof(format, args...)
	}
	if w.logf != nil {
		w.logf("[hl-ws] "+format, args...)
	}
}

func (w *WebsocketClient) logDebugf(fmt string, args ...any) {
	if w.logger == nil {
		return
	}

	w.logger.Debugf(fmt, args...)
}
