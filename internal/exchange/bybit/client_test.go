package bybit

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestCanonicalQuerySortsKeys(t *testing.T) {
	got := canonicalQuery(map[string]string{"symbol": "BTCUSDT", "category": "linear"})
	want := "category=linear&symbol=BTCUSDT"
	if got != want {
		t.Fatalf("canonicalQuery()=%q want %q", got, want)
	}
}

func TestSign(t *testing.T) {
	got := sign("secret", "payload")
	want := "b82fcb791acec57859b989b430a826488ce2e479fdf92326bd0a2e8375a42ba4"
	if got != want {
		t.Fatalf("sign()=%q want %q", got, want)
	}
}

func TestInstrumentRuleRoundQty(t *testing.T) {
	rule := instrumentRule{QtyStep: decimal.RequireFromString("0.001")}
	got := rule.roundQty(decimal.RequireFromString("1.23456"))
	want := decimal.RequireFromString("1.234")
	if !got.Equal(want) {
		t.Fatalf("roundQty()=%s want %s", got, want)
	}
}

func TestMergeOrderbookSnapshotAndDelta(t *testing.T) {
	c := &Client{books: make(map[string]*localBook)}
	snap := c.mergeOrderbook("snapshot", bybitOrderbook{Symbol: "BTCUSDT", Bids: [][]string{{"100", "1"}}, Asks: [][]string{{"101", "2"}}}, 1000)
	if len(snap.Bids) != 1 || len(snap.Asks) != 1 {
		t.Fatalf("unexpected snapshot depths: bids=%d asks=%d", len(snap.Bids), len(snap.Asks))
	}
	delta := c.mergeOrderbook("delta", bybitOrderbook{Symbol: "BTCUSDT", Bids: [][]string{{"100", "0"}, {"99", "3"}}, Asks: [][]string{{"101", "1.5"}}}, 2000)
	if len(delta.Bids) != 1 || !delta.Bids[0].Price.Equal(decimal.NewFromInt(99)) || !delta.Bids[0].Quantity.Equal(decimal.NewFromInt(3)) {
		t.Fatalf("unexpected bids after delta: %+v", delta.Bids)
	}
	if len(delta.Asks) != 1 || !delta.Asks[0].Quantity.Equal(decimal.RequireFromString("1.5")) {
		t.Fatalf("unexpected asks after delta: %+v", delta.Asks)
	}
}
