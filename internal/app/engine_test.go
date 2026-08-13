package app

import (
	"context"
	"testing"
	"time"

	"algos/internal/config"
	"algos/internal/model"
	"algos/internal/monitor"
	"algos/internal/order"
	"algos/internal/risk"
	"algos/internal/storage"

	"github.com/shopspring/decimal"
	"go.uber.org/zap/zaptest"
)

func TestFullPipeline_SignalToClose(t *testing.T) {
	store, err := storage.OpenBolt(t.TempDir() + "/trades.db")
	if err != nil {
		t.Fatalf("open bolt: %v", err)
	}
	defer store.Close()

	cfg := &config.Config{
		Bot: config.BotConfig{
			TradingMode:        "paper",
			MaxLeverage:        3,
			TPPercent:          decimal.NewFromInt(4),
			SLPercent:          decimal.RequireFromString("1.5"),
			MaxPositionSeconds: 20,
			KillSwitchLosses:   3,
		},
		Strategy: config.StrategyConfig{
			OpeningWindowSeconds:     5,
			TradeVelocityRatio:       decimal.RequireFromString("1.2"),
			ImbalanceRatio:           decimal.RequireFromString("1.1"),
			MaxPositionPctLiquidity:  decimal.NewFromInt(10),
			LiquidityRadiusPercent:   decimal.RequireFromString("1.5"),
			OrderbookRadiusPercent:   decimal.NewFromInt(2),
			DEXFairPriceEnabled:      false,
			ShortOverpriceMultiplier: decimal.NewFromInt(3),
		},
	}

	riskManager := risk.NewManager(cfg.Bot)
	orderManager := order.NewManager(cfg, nil, riskManager, store, monitor.NoopNotifier{}, zaptest.NewLogger(t))
	engine := NewEngine(cfg, riskManager, orderManager, zaptest.NewLogger(t))

	listingTime := time.Now().UTC()
	engine.RegisterListing("bybit", "TESTUSDT", listingTime, decimal.NewFromInt(1))

	// Simulate incoming trades: heavy buy pressure
	for i := 0; i < 10; i++ {
		engine.OnTrade(model.Trade{
			Exchange:  "bybit",
			Symbol:    "TESTUSDT",
			Side:      model.SideBuy,
			Price:     decimal.NewFromInt(1),
			Quantity:  decimal.NewFromInt(1000),
			Timestamp: listingTime.Add(time.Duration(i) * 100 * time.Millisecond),
		})
	}

	// Simulate orderbook with bid dominance
	engine.OnOrderbook(model.Orderbook{
		Exchange: "bybit",
		Symbol:   "TESTUSDT",
		Bids: []model.OrderbookLevel{
			{Price: decimal.NewFromInt(1), Quantity: decimal.NewFromInt(10000)},
			{Price: decimal.RequireFromString("0.99"), Quantity: decimal.NewFromInt(5000)},
		},
		Asks: []model.OrderbookLevel{
			{Price: decimal.NewFromInt(1).Add(decimal.RequireFromString("0.01")), Quantity: decimal.NewFromInt(2000)},
			{Price: decimal.NewFromInt(1).Add(decimal.RequireFromString("0.02")), Quantity: decimal.NewFromInt(1000)},
		},
		Timestamp: listingTime.Add(time.Second),
	})

	// Check if position was opened (paper mode)
	if !orderManager.HasPosition() {
		t.Fatal("expected position to be opened by strategy signal")
	}
	t.Log("Position opened successfully in paper mode!")

	// Simulate price going up to trigger TP
	orderManager.CheckWithPrice(context.Background(), decimal.RequireFromString("1.04"), time.Now().UTC())
	if orderManager.HasPosition() {
		t.Fatal("position should be closed by TP at +4%")
	}
	t.Log("Position closed by TAKE_PROFIT!")
}
