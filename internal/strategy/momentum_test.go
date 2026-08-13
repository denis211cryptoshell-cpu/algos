package strategy

import (
	"testing"
	"time"

	"algos/internal/config"
	"algos/internal/model"

	"github.com/shopspring/decimal"
)

func TestMomentumStrategyEnterLong(t *testing.T) {
	now := time.Now().UTC()
	s := NewMomentumStrategy(defaultStrategyConfig())
	signal := s.Evaluate(model.MarketSnapshot{
		Exchange:     "bybit",
		Symbol:       "NEWUSDT",
		OpeningPrice: d("1"),
		LastPrice:    d("1"),
		ListingTime:  now.Add(-2 * time.Second),
		Now:          now,
		Trades: []model.Trade{
			{Side: model.SideBuy, Price: d("1"), Quantity: d("2000"), Timestamp: now.Add(-1 * time.Second)},
			{Side: model.SideSell, Price: d("1"), Quantity: d("1000"), Timestamp: now.Add(-1 * time.Second)},
		},
		Orderbook: model.Orderbook{
			Bids: []model.OrderbookLevel{{Price: d("0.99"), Quantity: d("2000")}, {Price: d("0.98"), Quantity: d("1000")}},
			Asks: []model.OrderbookLevel{{Price: d("1.01"), Quantity: d("1000")}},
		},
	})

	if signal.Action != model.SignalEnterLong {
		t.Fatalf("expected long, got %s: %s", signal.Action, signal.Reason)
	}
	if !signal.Quantity.Equal(d("101")) {
		t.Fatalf("expected qty 101, got %s", signal.Quantity)
	}
}

func TestMomentumStrategyEnterShort(t *testing.T) {
	now := time.Now().UTC()
	cfg := defaultStrategyConfig()
	cfg.DEXFairPriceEnabled = true
	cfg.DEXFairPrice = d("1")
	s := NewMomentumStrategy(cfg)
	signal := s.Evaluate(model.MarketSnapshot{
		Exchange:     "bybit",
		Symbol:       "NEWUSDT",
		OpeningPrice: d("2.5"),
		LastPrice:    d("2.5"),
		ListingTime:  now.Add(-2 * time.Second),
		Now:          now,
		Trades: []model.Trade{
			{Side: model.SideSell, Price: d("2.5"), Quantity: d("2000"), Timestamp: now.Add(-1 * time.Second)},
			{Side: model.SideBuy, Price: d("2.5"), Quantity: d("1000"), Timestamp: now.Add(-1 * time.Second)},
		},
		Orderbook: model.Orderbook{
			Bids: []model.OrderbookLevel{{Price: d("2.49"), Quantity: d("1000")}},
			Asks: []model.OrderbookLevel{{Price: d("2.51"), Quantity: d("2000")}},
		},
	})

	if signal.Action != model.SignalEnterShort {
		t.Fatalf("expected short, got %s: %s", signal.Action, signal.Reason)
	}
}

func TestMomentumStrategyHoldOutsideOpeningWindow(t *testing.T) {
	now := time.Now().UTC()
	s := NewMomentumStrategy(defaultStrategyConfig())
	signal := s.Evaluate(model.MarketSnapshot{
		Exchange:    "bybit",
		Symbol:      "NEWUSDT",
		LastPrice:   d("1"),
		ListingTime: now.Add(-4 * time.Second),
		Now:         now,
	})
	if signal.Action != model.SignalHold {
		t.Fatalf("expected hold, got %s", signal.Action)
	}
}

func defaultStrategyConfig() config.StrategyConfig {
	return config.StrategyConfig{
		ImbalanceRatio:           d("1.3"),
		TradeVelocityRatio:       d("1.5"),
		LiquidityRadiusPercent:   d("1.5"),
		OrderbookRadiusPercent:   d("2.0"),
		MaxPositionPctLiquidity:  d("10"),
		OpeningWindowSeconds:     3,
		DEXFairPriceEnabled:      false,
		DEXFairPrice:             decimal.Zero,
		ShortOverpriceMultiplier: d("2.5"),
	}
}

func d(value string) decimal.Decimal {
	parsed, err := decimal.NewFromString(value)
	if err != nil {
		panic(err)
	}
	return parsed
}