package hyperliquid

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// encodeCancelActionHex mirrors exactly what actionHash does before it appends the
// nonce, so these vectors pin the bytes the signature is derived from. Same helper
// shape as encodeActionHex in order_grouping_test.go.
func encodeCancelActionHex(t *testing.T, action any) string {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)
	if err := enc.Encode(action); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return hex.EncodeToString(convertStr16ToStr8(buf.Bytes()))
}

// The `f` flag is the highest-consequence byte in this file: it rides in the hashed
// action, so getting its placement or omission wrong does not degrade cancels, it
// makes every cancel fail signature verification. Two invariants, both pinned:
//
//	fast=false -> the field is ABSENT and the encoding is byte-identical to what
//	              the struct produced before `Fast` existed. HyperCore rejects
//	              actions hashed with an explicit `f: false`, so omitempty is not
//	              cosmetic.
//	fast=true  -> `f` is present, LAST, as msgpack true (a166 c3), matching the
//	              documented action shape {"type","cancels","f"}.
func TestCancelActionFastFlagMsgpack(t *testing.T) {
	// Pre-`Fast` shape, kept here as the frozen baseline for the false case.
	type legacyCancelAction struct {
		Type    string            `msgpack:"type"`
		Dex     string            `msgpack:"dex,omitempty"`
		Cancels []CancelOrderWire `msgpack:"cancels"`
	}
	wires := []CancelOrderWire{{Asset: 0, OrderID: 123}}

	legacy := encodeCancelActionHex(t, legacyCancelAction{Type: "cancel", Cancels: wires})
	off := encodeCancelActionHex(t, CancelAction{Type: "cancel", Cancels: wires, Fast: false})
	on := encodeCancelActionHex(t, CancelAction{Type: "cancel", Cancels: wires, Fast: true})

	if off != legacy {
		t.Fatalf("fast=false changed the hashed bytes; every existing cancel signature would break\n legacy=%s\n    got=%s", legacy, off)
	}
	// fixmap(2) -> fixmap(3), and the new pair appended at the end.
	if !strings.HasPrefix(legacy, "82") {
		t.Fatalf("baseline is not a 2-entry fixmap: %s", legacy)
	}
	wantOn := "83" + strings.TrimPrefix(legacy, "82") + "a166c3"
	if on != wantOn {
		t.Fatalf("fast=true encoding is not <3-entry map>+<legacy body>+f:true\n want=%s\n  got=%s", wantOn, on)
	}
	if strings.Contains(off, "a166") {
		t.Fatalf("fast=false must not emit the `f` key at all: %s", off)
	}
}

// Same two invariants for cancelByCloid — it is a separate action with its own
// wire shape (`asset`/`cloid`, not `a`/`o`), so it needs its own pin.
func TestCancelByCloidActionFastFlagMsgpack(t *testing.T) {
	type legacyCancelByCloidAction struct {
		Type    string              `msgpack:"type"`
		Dex     string              `msgpack:"dex,omitempty"`
		Cancels []CancelByCloidWire `msgpack:"cancels"`
	}
	wires := []CancelByCloidWire{{Asset: 0, ClientID: "0x00000000000000000000000000000001"}}

	legacy := encodeCancelActionHex(t, legacyCancelByCloidAction{Type: "cancelByCloid", Cancels: wires})
	off := encodeCancelActionHex(t, CancelByCloidAction{Type: "cancelByCloid", Cancels: wires, Fast: false})
	on := encodeCancelActionHex(t, CancelByCloidAction{Type: "cancelByCloid", Cancels: wires, Fast: true})

	if off != legacy {
		t.Fatalf("fast=false changed the hashed bytes\n legacy=%s\n    got=%s", legacy, off)
	}
	wantOn := "83" + strings.TrimPrefix(legacy, "82") + "a166c3"
	if on != wantOn {
		t.Fatalf("fast=true encoding wrong\n want=%s\n  got=%s", wantOn, on)
	}
}

// The option plumbing must actually reach the action — a WithFastCancel() that
// silently does nothing would look exactly like "the flag has no effect".
func TestWithFastCancelOptionReachesAction(t *testing.T) {
	if applyCancelOpts(nil).fast {
		t.Fatalf("no options must mean fast=false")
	}
	if !applyCancelOpts([]CancelOpt{WithFastCancel()}).fast {
		t.Fatalf("WithFastCancel() did not set fast")
	}
	if applyCancelOpts([]CancelOpt{nil}).fast {
		t.Fatalf("a nil option must be ignored, not panic or set fast")
	}
}

// The signature is computed over msgpack, but what actually travels to HyperCore
// is JSON — and this package ships easyjson-generated marshalers for the action
// types. A field added to the struct without regenerating them serializes into the
// HASH but not into the PAYLOAD, so the server recomputes a different hash and the
// signature recovers to a garbage address ("User or API Wallet 0x… does not
// exist"). That failure looks like "the exchange rejects the flag", which is why
// pinning msgpack alone is not enough.
func TestCancelActionFastFlagJSON(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action any
		wantF  bool
	}{
		{"cancel fast", CancelAction{Type: "cancel", Cancels: []CancelOrderWire{{Asset: 0, OrderID: 1}}, Fast: true}, true},
		{"cancel plain", CancelAction{Type: "cancel", Cancels: []CancelOrderWire{{Asset: 0, OrderID: 1}}}, false},
		{"cancelByCloid fast", CancelByCloidAction{Type: "cancelByCloid", Cancels: []CancelByCloidWire{{Asset: 0, ClientID: "0x1"}}, Fast: true}, true},
		{"cancelByCloid plain", CancelByCloidAction{Type: "cancelByCloid", Cancels: []CancelByCloidWire{{Asset: 0, ClientID: "0x1"}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.action)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			got := strings.Contains(string(b), `"f":true`)
			if got != tc.wantF {
				t.Fatalf("JSON payload carries f=%v, want %v — msgpack and JSON disagree, "+
					"so the signed action and the sent action are different documents: %s", got, tc.wantF, b)
			}
			if !tc.wantF && strings.Contains(string(b), `"f"`) {
				t.Fatalf("plain cancel must omit `f` entirely: %s", b)
			}
		})
	}
}
