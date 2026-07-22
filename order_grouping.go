package hyperliquid

// NOTE: deliberately no `//go:generate easyjson -all` here. OrderGrouping
// hand-rolls MarshalJSON/UnmarshalJSON and easyjson -all would emit colliding
// methods for it.

import (
	json "encoding/json"
	"fmt"
	"strconv"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	// PriorityRateDenominator is the denominator HyperCore applies to the
	// priority grouping's `p` value: the fee charged is p/1e8 of notional.
	PriorityRateDenominator = 100_000_000

	// PriorityRatePerBps is the `p` value worth one basis point of notional
	// (1bp = 0.0001 = 10_000/1e8).
	PriorityRatePerBps = PriorityRateDenominator / 10_000
)

// OrderGrouping is the `grouping` field of an order action: either one of the
// named groupings ("na", "normalTpsl", "positionTpsl") or the priority-fee form
// {"p": rate}, which bids for execution priority on HyperCore.
//
// The zero value is the "na" grouping, so an OrderAction that never sets
// Grouping serializes exactly as it did when this field was a plain string.
//
// Both encodings below are signature-relevant. The action is msgpack-encoded
// and hashed to build the payload that gets signed, and the same action travels
// to the exchange as JSON; the two must describe the same value, and the
// msgpack bytes must match what msgpack-python emits for the equivalent value
// or the exchange derives a different hash and rejects the signature.
// TestOrderGroupingMsgpack pins the reference vectors.
type OrderGrouping struct {
	name Grouping
	rate int64
}

// NamedGrouping returns one of the named groupings. The zero Grouping is
// normalized to GroupingNA.
func NamedGrouping(name Grouping) OrderGrouping {
	return OrderGrouping{name: name}
}

// PriorityGrouping returns the grouping that pays an order priority fee, where
// rate is a fraction rate/1e8 of notional — use PriorityRatePerBps to express
// it in basis points. HyperCore charges the fee against the signer's
// undelegated staking balance in HYPE: on filled notional for an IOC batch, on
// resting notional for a non-reduce-only ALO batch. Batches that mix time-in-
// force, or that touch outcome assets, are not eligible and the exchange
// rejects them.
func PriorityGrouping(rate int64) OrderGrouping {
	return OrderGrouping{rate: rate}
}

// IsPriority reports whether this grouping pays an order priority fee.
func (g OrderGrouping) IsPriority() bool { return g.rate > 0 }

// Rate returns the priority rate and whether one is set.
func (g OrderGrouping) Rate() (int64, bool) { return g.rate, g.rate > 0 }

// named resolves the zero value to GroupingNA.
func (g OrderGrouping) named() Grouping {
	if g.name == "" {
		return GroupingNA
	}
	return g.name
}

func (g OrderGrouping) String() string {
	if g.rate > 0 {
		return fmt.Sprintf("priority(%d)", g.rate)
	}
	return string(g.named())
}

// Validate rejects groupings HyperCore would not accept.
func (g OrderGrouping) Validate() error {
	if g.rate < 0 {
		return fmt.Errorf("hyperliquid: priority rate %d must be positive", g.rate)
	}
	if g.rate > 0 {
		if g.name != "" {
			return fmt.Errorf("hyperliquid: grouping cannot be both %q and a priority rate", g.name)
		}
		if g.rate > PriorityRateDenominator {
			return fmt.Errorf(
				"hyperliquid: priority rate %d exceeds 100%% of notional (%d)",
				g.rate, PriorityRateDenominator,
			)
		}
		return nil
	}
	switch g.named() {
	case GroupingNA, GroupingNormalTpsl, GroupingPositionTpls:
		return nil
	default:
		return fmt.Errorf("hyperliquid: unknown grouping %q", g.name)
	}
}

func (g OrderGrouping) MarshalJSON() ([]byte, error) {
	if g.rate > 0 {
		return []byte(`{"p":` + strconv.FormatInt(g.rate, 10) + `}`), nil
	}
	return json.Marshal(string(g.named()))
}

func (g *OrderGrouping) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err == nil {
		*g = OrderGrouping{name: Grouping(name)}
		return nil
	}
	var priority struct {
		P *int64 `json:"p"`
	}
	if err := json.Unmarshal(data, &priority); err != nil || priority.P == nil {
		return fmt.Errorf("hyperliquid: cannot decode grouping from %s", data)
	}
	*g = OrderGrouping{rate: *priority.P}
	return nil
}

// EncodeMsgpack writes the grouping the way msgpack-python's packb does: a bare
// string for the named forms, a single-entry map for the priority form. The
// integer takes msgpack's smallest representation, matching both msgpack-python
// and the UseCompactInts(true) encoder actionHash already uses for the rest of
// the action.
func (g OrderGrouping) EncodeMsgpack(enc *msgpack.Encoder) error {
	if g.rate > 0 {
		if err := enc.EncodeMapLen(1); err != nil {
			return err
		}
		if err := enc.EncodeString("p"); err != nil {
			return err
		}
		return enc.EncodeInt(g.rate)
	}
	return enc.EncodeString(string(g.named()))
}
