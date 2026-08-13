package app

import (
	"context"
	"sync"
	"time"

	"algos/internal/config"
	"algos/internal/exchange"
	"algos/internal/model"
	"algos/internal/order"
	"algos/internal/risk"
	"algos/internal/strategy"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const maxBufferedTrades = 2048
const maxBufferedOrderbooks = 100

type Engine struct {
	cfg      *config.Config
	strategy *strategy.MomentumStrategy
	risk     *risk.Manager
	orders   *order.Manager
	logger   *zap.Logger

	mu      sync.Mutex
	markets map[string]*marketState

	tradeCount     int64
	orderbookCount int64
	lastLogTime    time.Time
}

type marketState struct {
	listingTime  time.Time
	openingPrice decimal.Decimal
	lastPrice    decimal.Decimal
	trades       []model.Trade
	orderbook    model.Orderbook
	orderbooks   []model.Orderbook
	entered      bool
}

func NewEngine(cfg *config.Config, riskManager *risk.Manager, orders *order.Manager, logger *zap.Logger) *Engine {
	return &Engine{
		cfg:      cfg,
		strategy: strategy.NewMomentumStrategy(cfg.Strategy),
		risk:     riskManager,
		orders:   orders,
		logger:   logger.Named("engine"),
		markets:  make(map[string]*marketState),
	}
}

func (e *Engine) RegisterListing(exchangeName, symbol string, listingTime time.Time, openingPrice decimal.Decimal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.markets[marketKey(exchangeName, symbol)] = &marketState{listingTime: listingTime, openingPrice: openingPrice, lastPrice: openingPrice}
	e.logger.Info("registered listing", zap.String("exchange", exchangeName), zap.String("symbol", symbol), zap.Time("listing_time", listingTime), zap.String("opening_price", openingPrice.String()))
}

func (e *Engine) OnTrade(trade model.Trade) {
	e.mu.Lock()
	state := e.ensureMarketLocked(trade.Exchange, trade.Symbol, trade.Timestamp, trade.Price)
	state.lastPrice = trade.Price
	state.trades = append(state.trades, trade)
	if len(state.trades) > maxBufferedTrades {
		state.trades = state.trades[len(state.trades)-maxBufferedTrades:]
	}
	snapshot := e.snapshotLocked(trade.Exchange, trade.Symbol, state, trade.Timestamp)
	e.tradeCount++
	e.maybeLogActivity()
	e.mu.Unlock()

	e.evaluate(context.Background(), snapshot)
}

func (e *Engine) OnOrderbook(orderbook model.Orderbook) {
	e.mu.Lock()
	state := e.ensureMarketLocked(orderbook.Exchange, orderbook.Symbol, orderbook.Timestamp, decimal.Zero)
	state.orderbook = orderbook
	state.orderbooks = append(state.orderbooks, orderbook)
	if len(state.orderbooks) > maxBufferedOrderbooks {
		state.orderbooks = state.orderbooks[len(state.orderbooks)-maxBufferedOrderbooks:]
	}
	if !state.lastPrice.IsPositive() {
		state.lastPrice = bestMid(orderbook)
	}
	snapshot := e.snapshotLocked(orderbook.Exchange, orderbook.Symbol, state, orderbook.Timestamp)
	e.orderbookCount++
	e.maybeLogActivity()
	e.mu.Unlock()

	e.evaluate(context.Background(), snapshot)
}

func (e *Engine) maybeLogActivity() {
	now := time.Now()
	if now.Sub(e.lastLogTime) < 5*time.Second {
		return
	}
	e.lastLogTime = now
	e.logger.Info("activity",
		zap.Int64("trades", e.tradeCount),
		zap.Int64("orderbooks", e.orderbookCount),
	)
}

func (e *Engine) evaluate(ctx context.Context, snapshot model.MarketSnapshot) {
	if !e.orders.CanEnter() {
		return
	}
	if !snapshot.LastPrice.IsPositive() || len(snapshot.Orderbook.Bids) == 0 || len(snapshot.Orderbook.Asks) == 0 {
		return
	}

	signal := e.strategy.Evaluate(snapshot)
	if signal.Action == model.SignalHold || !signal.Quantity.IsPositive() {
		return
	}
	if err := e.orders.SaveSignal(signal); err != nil {
		e.logger.Warn("save signal", zap.Error(err))
	}

	e.mu.Lock()
	state := e.markets[marketKey(snapshot.Exchange, snapshot.Symbol)]
	if state == nil || state.entered {
		e.mu.Unlock()
		return
	}
	state.entered = true
	e.mu.Unlock()

	position, err := e.orders.OpenFromSignal(ctx, signal)
	if err != nil {
		e.logger.Warn("signal rejected", zap.String("symbol", signal.Symbol), zap.Error(err))
		e.mu.Lock()
		if state := e.markets[marketKey(snapshot.Exchange, snapshot.Symbol)]; state != nil {
			state.entered = false
		}
		e.mu.Unlock()
		return
	}
	e.logger.Info("position opened", zap.String("id", position.ID), zap.String("side", string(position.Side)), zap.String("price", position.EntryPrice.String()), zap.String("qty", position.Quantity.String()), zap.String("reason", signal.Reason))
}

func (e *Engine) CheckOpenPosition(ctx context.Context, markPrice decimal.Decimal, now time.Time) {
	e.orders.CheckWithPrice(ctx, markPrice, now)
}

func (e *Engine) ensureMarketLocked(exchangeName, symbol string, ts time.Time, price decimal.Decimal) *marketState {
	key := marketKey(exchangeName, symbol)
	state := e.markets[key]
	if state == nil {
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		state = &marketState{listingTime: ts, openingPrice: price, lastPrice: price}
		e.markets[key] = state
	}
	return state
}

func (e *Engine) snapshotLocked(exchangeName, symbol string, state *marketState, now time.Time) model.MarketSnapshot {
	trades := make([]model.Trade, len(state.trades))
	copy(trades, state.trades)
	orderbooks := make([]model.Orderbook, len(state.orderbooks))
	copy(orderbooks, state.orderbooks)
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return model.MarketSnapshot{
		Exchange:     exchangeName,
		Symbol:       symbol,
		OpeningPrice: state.openingPrice,
		LastPrice:    state.lastPrice,
		ListingTime:  state.listingTime,
		Now:          now,
		Trades:       trades,
		Orderbook:    state.orderbook,
		Orderbooks:   orderbooks,
	}
}

func marketKey(exchangeName, symbol string) string {
	return exchangeName + ":" + symbol
}

func bestMid(orderbook model.Orderbook) decimal.Decimal {
	if len(orderbook.Bids) == 0 || len(orderbook.Asks) == 0 {
		return decimal.Zero
	}
	return orderbook.Bids[0].Price.Add(orderbook.Asks[0].Price).Div(decimal.NewFromInt(2))
}

var _ exchange.MarketDataHandler = (*Engine)(nil)
