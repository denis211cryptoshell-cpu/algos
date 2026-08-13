package order

import (
	"context"
	"testing"
	"time"

	"algos/internal/config"
	"algos/internal/model"
	"algos/internal/monitor"
	"algos/internal/risk"
	"algos/internal/storage"

	"github.com/shopspring/decimal"
	"go.uber.org/zap/zaptest"
)

func TestPaperManagerOpenAndCloseByTakeProfit(t *testing.T) {
	store, err := storage.OpenBolt(t.TempDir() + "/trades.db")
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}()
	cfg := &config.Config{Bot: config.BotConfig{TradingMode: "paper", MaxLeverage: 3, TPPercent: decimal.NewFromInt(4), SLPercent: decimal.RequireFromString("1.5"), MaxPositionSeconds: 20, KillSwitchLosses: 3}}
	m := NewManager(cfg, nil, risk.NewManager(cfg.Bot), store, monitor.NoopNotifier{}, zaptest.NewLogger(t))
	signal := model.Signal{Action: model.SignalEnterLong, Exchange: "bybit", Symbol: "BTCUSDT", EntryPrice: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1), CreatedAt: time.Now().UTC()}
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
