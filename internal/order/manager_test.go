package order

import (
	"context"
	"testing"
	"time"

	"algos/internal/config"
	"algos/internal/exchange"
	"algos/internal/model"
	"algos/internal/monitor"
	"algos/internal/risk"
	"algos/internal/storage"

	"github.com/shopspring/decimal"
	"go.uber.org/zap/zaptest"
)

type fakeExec struct {
	mark decimal.Decimal
}

func (f *fakeExec) ConnectPublic(context.Context, []string, exchange.MarketDataHandler) error {
	return nil
}
func (f *fakeExec) PlaceOrder(context.Context, *model.Position) (string, error) { return "fake", nil }
func (f *fakeExec) PrepareSymbol(context.Context, string, int) error            { return nil }
func (f *fakeExec) GetMarkPrice(context.Context, string) (decimal.Decimal, error) {
	return f.mark, nil
}
func (f *fakeExec) ClosePosition(context.Context, string, model.Side) error { return nil }
func (f *fakeExec) ListSymbols(context.Context) ([]string, error)           { return nil, nil }
func (f *fakeExec) SubscribeSymbols([]string) error                         { return nil }

func testConfig() *config.Config {
	return &config.Config{Bot: config.BotConfig{TradingMode: "paper", MaxLeverage: 3, TPPercent: decimal.NewFromInt(4), SLPercent: decimal.RequireFromString("1.5"), MaxPositionSeconds: 20, KillSwitchLosses: 3}}
}

func openTestStore(t *testing.T) *storage.BoltStore {
	t.Helper()
	store, err := storage.OpenBolt(t.TempDir() + "/trades.db")
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	})
	return store
}

func TestPaperManagerOpenAndCloseByTakeProfit(t *testing.T) {
	store := openTestStore(t)
	cfg := testConfig()
	m := NewManager(cfg, nil, risk.NewManager(cfg.Bot), store, monitor.NoopNotifier{}, zaptest.NewLogger(t))
	signal := model.Signal{Action: model.SignalEnterLong, Exchange: "okx", Symbol: "BTCUSDT", EntryPrice: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1), CreatedAt: time.Now().UTC()}
	pos, err := m.OpenFromSignal(context.Background(), signal)
	if err != nil {
		t.Fatalf("open from signal: %v", err)
	}
	if pos.TpPrice.String() != "104" || pos.SlPrice.String() != "98.5" {
		t.Fatalf("unexpected tp/sl: tp=%s sl=%s", pos.TpPrice, pos.SlPrice)
	}
	m.CheckWithPrice(context.Background(), decimal.NewFromInt(104), time.Now().UTC())
	if m.HasPosition() {
		t.Fatal("position should be closed by TP")
	}
}

// Regression: checkPosition used to skip paper mode entirely, so paper
// positions never hit TP/SL/max-lifetime.
func TestPaperCheckPositionClosesByTakeProfit(t *testing.T) {
	store := openTestStore(t)
	cfg := testConfig()
	exec := &fakeExec{mark: decimal.NewFromInt(104)}
	m := NewManager(cfg, exec, risk.NewManager(cfg.Bot), store, monitor.NoopNotifier{}, zaptest.NewLogger(t))
	signal := model.Signal{Action: model.SignalEnterLong, Exchange: "okx", Symbol: "BTCUSDT", EntryPrice: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1), CreatedAt: time.Now().UTC()}
	if _, err := m.OpenFromSignal(context.Background(), signal); err != nil {
		t.Fatalf("open from signal: %v", err)
	}
	m.checkPosition(context.Background(), time.Now().UTC())
	if m.HasPosition() {
		t.Fatal("paper position should be closed by TP via checkPosition")
	}
}

func TestRestoreOpenPositionFromStorage(t *testing.T) {
	store := openTestStore(t)
	cfg := testConfig()
	riskManager := risk.NewManager(cfg.Bot)
	m := NewManager(cfg, nil, riskManager, store, monitor.NoopNotifier{}, zaptest.NewLogger(t))
	signal := model.Signal{Action: model.SignalEnterLong, Exchange: "okx", Symbol: "BTCUSDT", EntryPrice: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1), CreatedAt: time.Now().UTC()}
	opened, err := m.OpenFromSignal(context.Background(), signal)
	if err != nil {
		t.Fatalf("open from signal: %v", err)
	}

	open, err := store.LoadOpenPositions()
	if err != nil {
		t.Fatalf("load open positions: %v", err)
	}
	if len(open) != 1 || open[0].ID != opened.ID {
		t.Fatalf("expected one open position %q, got %+v", opened.ID, open)
	}

	restoredManager := NewManager(cfg, nil, risk.NewManager(cfg.Bot), store, monitor.NoopNotifier{}, zaptest.NewLogger(t))
	if err := restoredManager.RestorePosition(open[0]); err != nil {
		t.Fatalf("restore position: %v", err)
	}
	if !restoredManager.HasPosition() {
		t.Fatal("restored manager should report an open position")
	}
	if restoredManager.CanEnter() {
		t.Fatal("restored manager must not allow a new entry")
	}

	if err := restoredManager.RestorePosition(open[0]); err == nil {
		t.Fatal("second restore should fail while a position is open")
	}
}
