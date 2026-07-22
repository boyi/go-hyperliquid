package hyperliquid

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// encodeActionHex mirrors exactly what actionHash does before it appends the
// nonce, so these vectors pin the bytes the signature is derived from.
func encodeActionHex(t *testing.T, action OrderAction) string {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)
	if err := enc.Encode(action); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return hex.EncodeToString(convertStr16ToStr8(buf.Bytes()))
}

func priorityTestAction(tif Tif, grouping OrderGrouping) OrderAction {
	return OrderAction{
		Type: "order",
		Orders: []OrderWire{{
			Asset:      0,
			IsBuy:      true,
			LimitPx:    "40000",
			Size:       "0.001",
			ReduceOnly: false,
			OrderType:  OrderWireType{Limit: &OrderWireTypeLimit{Tif: tif}},
		}},
		Grouping: grouping,
	}
}

// TestOrderGroupingMsgpack pins the msgpack bytes against msgpack-python, which
// is what HyperCore hashes to verify the signature. The expected values come
// from msgpack.packb() over the equivalent Python dict — see the Python SDK's
// order_wires_to_order_action, where grouping is either a string or {"p": int}.
func TestOrderGroupingMsgpack(t *testing.T) {
	const wirePrefix = "83a474797065a56f72646572a66f72646572739186a16100a162c3a170a5343030303" +
		"0a173a5302e303031a172c2a17481a56c696d697481a3746966a3"

	tests := []struct {
		name     string
		tif      Tif
		grouping OrderGrouping
		want     string
	}{
		{
			// Regression guard: the zero value must encode identically to the
			// plain "na" string this field held before OrderGrouping existed.
			name:     "zero value is na",
			tif:      TifGtc,
			grouping: OrderGrouping{},
			want:     wirePrefix + "477463" + "a867726f7570696e67a26e61",
		},
		{
			name:     "explicit na",
			tif:      TifGtc,
			grouping: NamedGrouping(GroupingNA),
			want:     wirePrefix + "477463" + "a867726f7570696e67a26e61",
		},
		{
			// 1bp: uint16, matching msgpack-python's smallest-width choice.
			name:     "priority 1bp",
			tif:      TifIoc,
			grouping: PriorityGrouping(PriorityRatePerBps),
			want:     wirePrefix + "496f63" + "a867726f7570696e6781a170cd2710",
		},
		{
			// 8bp: crosses into uint32.
			name:     "priority 8bp",
			tif:      TifIoc,
			grouping: PriorityGrouping(8 * PriorityRatePerBps),
			want:     wirePrefix + "496f63" + "a867726f7570696e6781a170ce00013880",
		},
		{
			name:     "priority 100 percent",
			tif:      TifIoc,
			grouping: PriorityGrouping(PriorityRateDenominator),
			want:     wirePrefix + "496f63" + "a867726f7570696e6781a170ce05f5e100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeActionHex(t, priorityTestAction(tt.tif, tt.grouping))
			if got != tt.want {
				t.Errorf("msgpack does NOT match Python SDK\ngot:  %s\nwant: %s", got, tt.want)
			}
		})
	}
}

// TestOrderGroupingJSON pins the JSON body that travels to /exchange. It has to
// describe the same value as the msgpack above, or HyperCore re-encodes the
// received action and derives a different hash than the one we signed.
func TestOrderGroupingJSON(t *testing.T) {
	tests := []struct {
		name     string
		grouping OrderGrouping
		want     string
	}{
		{"zero value", OrderGrouping{}, `"na"`},
		{"na", NamedGrouping(GroupingNA), `"na"`},
		{"normalTpsl", NamedGrouping(GroupingNormalTpsl), `"normalTpsl"`},
		{"priority 1bp", PriorityGrouping(PriorityRatePerBps), `{"p":10000}`},
		{"priority 8bp", PriorityGrouping(8 * PriorityRatePerBps), `{"p":80000}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.grouping)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}

			var back OrderGrouping
			if err := json.Unmarshal(got, &back); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			// Compare through the encoders rather than the unexported fields:
			// the zero value and an explicit "na" are the same wire value.
			roundTripped, err := json.Marshal(back)
			if err != nil {
				t.Fatalf("re-marshal: %v", err)
			}
			if string(roundTripped) != tt.want {
				t.Errorf("round trip got %s, want %s", roundTripped, tt.want)
			}
		})
	}
}

// TestOrderActionJSONWithPriority checks the grouping survives the easyjson
// marshaler generated for OrderAction, which is what actually builds the body.
func TestOrderActionJSONWithPriority(t *testing.T) {
	action := priorityTestAction(TifIoc, PriorityGrouping(PriorityRatePerBps))
	got, err := json.Marshal(action)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"type":"order","orders":[{"a":0,"b":true,"p":"40000","s":"0.001",` +
		`"r":false,"t":{"limit":{"tif":"Ioc"}}}],"grouping":{"p":10000}}`
	if string(got) != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}

	var back OrderAction
	if err := json.Unmarshal(got, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rate, ok := back.Grouping.Rate(); !ok || rate != PriorityRatePerBps {
		t.Errorf("decoded grouping = %v (rate %d, ok %v), want priority 10000",
			back.Grouping, rate, ok)
	}
}

func TestOrderGroupingValidate(t *testing.T) {
	valid := []OrderGrouping{
		{},
		NamedGrouping(GroupingNA),
		NamedGrouping(GroupingNormalTpsl),
		NamedGrouping(GroupingPositionTpls),
		PriorityGrouping(1),
		PriorityGrouping(PriorityRatePerBps),
		PriorityGrouping(PriorityRateDenominator),
	}
	for _, g := range valid {
		if err := g.Validate(); err != nil {
			t.Errorf("%v: unexpected error %v", g, err)
		}
	}

	invalid := []OrderGrouping{
		NamedGrouping("nope"),
		PriorityGrouping(-1),
		PriorityGrouping(PriorityRateDenominator + 1),
		{name: GroupingNA, rate: PriorityRatePerBps},
	}
	for _, g := range invalid {
		if err := g.Validate(); err == nil {
			t.Errorf("%v: expected an error, got nil", g)
		}
	}
}

// TestPriorityRateConstants guards the arithmetic behind the bps helper: the
// exchange charges p/1e8 of notional, so one basis point is 10_000.
func TestPriorityRateConstants(t *testing.T) {
	if PriorityRateDenominator != 100_000_000 {
		t.Errorf("PriorityRateDenominator = %d, want 1e8", PriorityRateDenominator)
	}
	if PriorityRatePerBps != 10_000 {
		t.Errorf("PriorityRatePerBps = %d, want 10000", PriorityRatePerBps)
	}
}
