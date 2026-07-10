package hyperliquid

import (
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sonirico/vago/lol"
)

type Opt[T any] func(*T)

func (o Opt[T]) Apply(opt *T) {
	o(opt)
}

type (
	ClientOpt   = Opt[client]
	ExchangeOpt = Opt[Exchange]
	InfoOpt     = Opt[Info]
	WsOpt       = Opt[WebsocketClient]
)

func WsOptDebugMode() WsOpt {
	return func(w *WebsocketClient) {
		w.debug = true
		w.logger = lol.NewZerolog(
			lol.WithLevel(lol.LevelTrace),
			lol.WithWriter(os.Stdout),
			lol.WithEnv(lol.EnvDev),
		)
	}
}

func InfoOptDebugMode() InfoOpt {
	return func(i *Info) {
		i.debug = true
	}
}

func ExchangeOptDebugMode() ExchangeOpt {
	return func(e *Exchange) {
		e.debug = true
	}
}

func clientOptDebugMode() ClientOpt {
	return func(c *client) {
		c.debug = true
		c.logger = lol.NewZerolog(
			lol.WithLevel(lol.LevelTrace),
			lol.WithWriter(os.Stderr),
			lol.WithEnv(lol.EnvDev),
		)
	}
}

// ExchangeOptClientOptions allows passing of ClientOpt to Client
func ExchangeOptClientOptions(opts ...ClientOpt) ExchangeOpt {
	return func(e *Exchange) {
		e.clientOpts = append(e.clientOpts, opts...)
	}
}

// ExchangeOptInfoOptions allows passing of InfoOpt to Info
func ExchangeOptInfoOptions(opts ...InfoOpt) ExchangeOpt {
	return func(e *Exchange) {
		e.infoOpts = append(e.infoOpts, opts...)
	}
}

func ExchangeOptPerpDex(dex string) ExchangeOpt {
	return func(e *Exchange) {
		e.dex = dex
		if dex != "" {
			e.infoOpts = append(e.infoOpts, InfoOptPerpDexName(dex))
		}
	}
}

func InfoOptPerpDexName(dex string) InfoOpt {
	return func(i *Info) {
		i.perpDexName = dex
	}
}

// ExchangeOptNonceFunc injects the nonce allocator. When nil, each Exchange
// allocates from its own lastNonce, which collides across Exchange values that
// share a signing key. See NonceFunc.
func ExchangeOptNonceFunc(f NonceFunc) ExchangeOpt {
	return func(e *Exchange) {
		e.nonceFunc = f
	}
}

// ExchangeOptPreSubmit injects a hook run just before each L1 action's nonce is
// allocated. Use it to do the waiting — rate limiting, admission control — on
// the near side of the nonce. See PreSubmitFunc.
func ExchangeOptPreSubmit(f PreSubmitFunc) ExchangeOpt {
	return func(e *Exchange) {
		e.preSubmit = f
	}
}

// ExchangeOptL1Signer injects an L1ActionSigner. When nil, the default ECDSA implementation with privateKey is used.
func ExchangeOptL1Signer(s L1ActionSigner) ExchangeOpt {
	return func(e *Exchange) {
		e.l1Signer = s
	}
}

// ExchangeOptUserSignedSigner injects a UserSignedActionSigner. When nil, the default ECDSA implementation with privateKey is used.
func ExchangeOptUserSignedSigner(s UserSignedActionSigner) ExchangeOpt {
	return func(e *Exchange) {
		e.userSignedSigner = s
	}
}

// ExchangeOptAgentSigner injects an AgentSigner. When nil, the default ECDSA implementation with privateKey is used.
func ExchangeOptAgentSigner(s AgentSigner) ExchangeOpt {
	return func(e *Exchange) {
		e.agentSigner = s
	}
}

// InfoOptClientOptions allows passing of ClientOpt to Info
func InfoOptClientOptions(opts ...ClientOpt) InfoOpt {
	return func(i *Info) {
		i.clientOpts = append(i.clientOpts, opts...)
	}
}

// WsOptReadTimeout sets the maximum duration to wait for a single read from the
// server. If no message is received within the timeout the connection is closed
// and a reconnection is attempted. Must exceed the internal ping interval (50 s).
// Defaults to 90 s.
func WsOptReadTimeout(timeout time.Duration) WsOpt {
	return func(w *WebsocketClient) {
		w.readTimeout = timeout
	}
}

// WsOptDialer allows setting a custom websocket.Dialer
func WsOptDialer(dialer *websocket.Dialer) WsOpt {
	return func(w *WebsocketClient) {
		w.dialer = dialer
	}
}

// ClientOptHTTPClient allows setting a custom http.Client
func ClientOptHTTPClient(httpClient *http.Client) ClientOpt {
	return func(c *client) {
		c.httpClient = httpClient
	}
}
