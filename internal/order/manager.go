package order

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"algos/internal/config"
	"algos/internal/exchange"
	"algos/internal/model"
	"algos/internal/monitor"
	"algos/internal/risk"
	"algos/internal/storage"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const latencyWindow = 256

type latencySample struct {
	decision time.Duration
	place    time.Duration
}

type Manager struct {
	cfg    *config.Config
	exec   exchange.Client
	risk   *risk.Manager
	store  *storage.BoltStore
	notify monitor.Notifier
	logger *zap.Logger
	paper  bool

	mu            sync.Mutex
	position      *model.Position
	prices        map[string]decimal.Decimal
	latency       []latencySample
	closing       bool
	closeFailures int
	closeNotified bool
}

func NewManager(cfg *config.Config, exec exchange.Client, riskManager *risk.Manager, store *storage.BoltStore, notifier monitor.Notifier, logger *zap.Logger) *Manager {
	return &Manager{
		cfg:    cfg,
		exec:   exec,
		risk:   riskManager,
		store:  store,
		notify: notifier,
		logger: logger.Named("order"),
		paper:  cfg.Bot.TradingMode != "live",
		prices: make(map[string]decimal.Decimal),
	}
}

// UpdateMarkPrice feeds the latest market price from the public websocket so
// position monitoring does not need REST polling.
func (m *Manager) UpdateMarkPrice(symbol string, price decimal.Decimal) {
	if !price.IsPositive() {
		return
	}
	m.mu.Lock()
	m.prices[symbol] = price
	m.mu.Unlock()
}

func (m *Manager) HasPosition() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.position != nil
}

func (m *Manager) CanEnter() bool {
	return m.risk.CanTrade() && !m.HasPosition()
}

func (m *Manager) SaveSignal(signal model.Signal) error {
	return m.store.SaveSignal(signal)
}

func (m *Manager) OpenFromSignal(ctx context.Context, signal model.Signal) (*model.Position, error) {
	if !m.risk.CanTrade() {
		return nil, fmt.Errorf("kill switch is active")
	}
	if signal.Quantity.IsZero() || signal.Quantity.IsNegative() {
		return nil, fmt.Errorf("signal quantity is not positive")
	}
	side := model.SideLong
	if signal.Action == model.SignalEnterShort {
		side = model.SideShort
	}
	pos := &model.Position{
		ID:           fmt.Sprintf("%s-%s-%d", signal.Exchange, signal.Symbol, signal.CreatedAt.UnixNano()),
		Exchange:     signal.Exchange,
		Symbol:       signal.Symbol,
		Side:         side,
		EntryPrice:   signal.EntryPrice,
		Quantity:     signal.Quantity,
		Leverage:     m.cfg.Bot.MaxLeverage,
		OpenedAt:     signal.CreatedAt,
		MaxHoldUntil: signal.CreatedAt.Add(m.cfg.MaxPositionDuration()),
		ExitReason:   model.ExitNone,
	}
	pos.Margin = pos.EntryPrice.Mul(pos.Quantity).Div(decimal.NewFromInt(int64(pos.Leverage)))
	pos.TpPrice, pos.SlPrice = virtualTPSL(pos.EntryPrice, pos.Side, m.cfg.Bot.TPPercent, m.cfg.Bot.SLPercent)

	m.mu.Lock()
	if m.position != nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("position already open")
	}
	m.position = pos
	m.mu.Unlock()

	if !m.paper {
		if err := m.exec.PrepareSymbol(ctx, pos.Symbol, pos.Leverage); err != nil {
			m.clearPosition()
			return nil, fmt.Errorf("prepare symbol: %w", err)
		}
		placeStart := time.Now()
		orderID, err := m.exec.PlaceOrder(ctx, pos)
		if err != nil {
			m.clearPosition()
			return nil, fmt.Errorf("place order: %w", err)
		}
		placeLatency := time.Since(placeStart)
		decisionLatency := placeStart.Sub(signal.CreatedAt)
		m.recordLatency(decisionLatency, placeLatency)
		m.logger.Info("order placed",
			zap.String("order_id", orderID),
			zap.String("symbol", pos.Symbol),
			zap.Duration("decision_latency", decisionLatency),
			zap.Duration("place_latency", placeLatency),
		)
	} else {
		m.logger.Info("paper order", zap.String("symbol", pos.Symbol), zap.String("side", string(pos.Side)))
	}

	if err := m.store.SavePosition(*pos); err != nil {
		m.logger.Error("save opened position", zap.Error(err))
	}
	if err := m.notify.Notify(ctx, fmt.Sprintf("🚀 SIGNAL: %s %s | Price: %s | Size: %s | Leverage: %dx", pos.Symbol, pos.Side, pos.EntryPrice, pos.Quantity, pos.Leverage)); err != nil {
		m.logger.Warn("notify signal", zap.Error(err))
	}
	return pos, nil
}

func (m *Manager) RunMonitor(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("order monitor panic", zap.Any("panic", r))
		}
	}()
	ticker := time.NewTicker(m.cfg.PositionCheckInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.checkPosition(ctx, now.UTC())
		}
	}
}

func (m *Manager) CheckWithPrice(ctx context.Context, markPrice decimal.Decimal, now time.Time) {
	m.closeIfNeeded(ctx, markPrice, now.UTC())
}

func (m *Manager) CloseAll(ctx context.Context) {
	m.mu.Lock()
	pos := m.position
	m.mu.Unlock()
	if pos == nil {
		return
	}
	m.closePosition(ctx, pos, pos.EntryPrice, time.Now().UTC(), model.ExitManual)
}

func (m *Manager) checkPosition(ctx context.Context, now time.Time) {
	m.mu.Lock()
	pos := m.position
	cached := decimal.Zero
	if pos != nil {
		cached = m.prices[pos.Symbol]
	}
	m.mu.Unlock()
	if pos == nil {
		return
	}

	mark := cached
	if !mark.IsPositive() {
		if m.exec == nil {
			return
		}
		var err error
		mark, err = m.exec.GetMarkPrice(ctx, pos.Symbol)
		if err != nil {
			m.logger.Warn("get mark price", zap.Error(err))
			return
		}
	}
	m.closeIfNeeded(ctx, mark, now)
}

func (m *Manager) recordLatency(decision, place time.Duration) {
	m.mu.Lock()
	m.latency = append(m.latency, latencySample{decision: decision, place: place})
	if len(m.latency) > latencyWindow {
		m.latency = m.latency[len(m.latency)-latencyWindow:]
	}
	samples := make([]latencySample, len(m.latency))
	copy(samples, m.latency)
	count := len(samples)
	m.mu.Unlock()

	if count%25 != 0 {
		return
	}
	decisions := make([]time.Duration, count)
	places := make([]time.Duration, count)
	for i, s := range samples {
		decisions[i] = s.decision
		places[i] = s.place
	}
	sort.Slice(decisions, func(i, j int) bool { return decisions[i] < decisions[j] })
	sort.Slice(places, func(i, j int) bool { return places[i] < places[j] })
	percentile := func(list []time.Duration, q float64) time.Duration {
		return list[int(float64(len(list)-1)*q)]
	}
	m.logger.Info("latency stats",
		zap.Int("orders", count),
		zap.Duration("decision_p50", percentile(decisions, 0.5)),
		zap.Duration("decision_p95", percentile(decisions, 0.95)),
		zap.Duration("place_p50", percentile(places, 0.5)),
		zap.Duration("place_p95", percentile(places, 0.95)),
	)
}

func (m *Manager) closeIfNeeded(ctx context.Context, markPrice decimal.Decimal, now time.Time) {
	m.mu.Lock()
	pos := m.position
	m.mu.Unlock()
	if pos == nil {
		return
	}
	shouldExit, reason, _ := m.risk.EvaluateExit(*pos, markPrice, now)
	if !shouldExit {
		return
	}
	m.closePosition(ctx, pos, markPrice, now, reason)
}

func (m *Manager) closePosition(ctx context.Context, pos *model.Position, exitPrice decimal.Decimal, now time.Time, reason model.ExitReason) {
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return
	}
	m.closing = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.closing = false
		m.mu.Unlock()
	}()

	if !m.paper {
		if err := m.exec.ClosePosition(ctx, pos.Symbol, pos.Side); err != nil {
			m.logger.Error("close position", zap.String("id", pos.ID), zap.Error(err))
			m.mu.Lock()
			m.closeFailures++
			failures := m.closeFailures
			first := !m.closeNotified
			if first {
				m.closeNotified = true
			}
			m.mu.Unlock()
			if first {
				if nerr := m.notify.Notify(ctx, fmt.Sprintf("⚠️ failed to close %s %s: %v — will retry", pos.Symbol, pos.Side, err)); nerr != nil {
					m.logger.Warn("notify close failure", zap.Error(nerr))
				}
			}
			m.logger.Warn("close attempt failed", zap.String("id", pos.ID), zap.Int("failures", failures))
			return
		}
		m.mu.Lock()
		m.closeFailures = 0
		m.closeNotified = false
		m.mu.Unlock()
	}
	closed := *pos
	closed.ClosedAt = &now
	closed.ExitPrice = exitPrice
	closed.ExitReason = reason
	closed.PnLPercent = risk.PnLPercent(closed, exitPrice)
	m.risk.RegisterClosedPosition(closed)
	m.clearPosition()

	if err := m.store.SavePosition(closed); err != nil {
		m.logger.Error("save closed position", zap.Error(err))
	}
	if err := m.store.SaveTrade(toStoredTrade(closed)); err != nil {
		m.logger.Error("save stored trade", zap.Error(err))
	}
	message := closeMessage(closed)
	if err := m.notify.Notify(ctx, message); err != nil {
		m.logger.Warn("notify close", zap.Error(err))
	}
	m.logger.Info("position closed", zap.String("id", closed.ID), zap.String("reason", string(reason)), zap.String("pnl_percent", closed.PnLPercent.StringFixed(4)))
	if m.risk.KillSwitchActive() {
		if err := m.notify.Notify(ctx, fmt.Sprintf("⛔ KILL SWITCH ACTIVATED. %d losses in a row.", m.cfg.Bot.KillSwitchLosses)); err != nil {
			m.logger.Warn("notify kill switch", zap.Error(err))
		}
	}
}

func (m *Manager) RestorePosition(pos model.Position) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.position != nil {
		return fmt.Errorf("position already open")
	}
	restored := pos
	m.position = &restored
	return nil
}

func (m *Manager) clearPosition() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.position = nil
}

func virtualTPSL(entry decimal.Decimal, side model.Side, tpPct, slPct decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
	tpMove := tpPct.Div(decimal.NewFromInt(100))
	slMove := slPct.Div(decimal.NewFromInt(100))
	if side == model.SideShort {
		return entry.Mul(decimal.NewFromInt(1).Sub(tpMove)), entry.Mul(decimal.NewFromInt(1).Add(slMove))
	}
	return entry.Mul(decimal.NewFromInt(1).Add(tpMove)), entry.Mul(decimal.NewFromInt(1).Sub(slMove))
}

func toStoredTrade(position model.Position) storage.StoredTrade {
	exitTime := time.Now().UTC()
	if position.ClosedAt != nil {
		exitTime = *position.ClosedAt
	}
	return storage.StoredTrade{ID: position.ID, Symbol: position.Symbol, Side: string(position.Side), EntryPrice: position.EntryPrice.String(), ExitPrice: position.ExitPrice.String(), Quantity: position.Quantity.String(), PnL: position.PnLPercent.String(), EntryTime: position.OpenedAt, ExitTime: exitTime, Duration: exitTime.Sub(position.OpenedAt).Milliseconds(), Reason: string(position.ExitReason)}
}

func closeMessage(position model.Position) string {
	duration := int64(0)
	if position.ClosedAt != nil {
		duration = position.ClosedAt.Sub(position.OpenedAt).Milliseconds() / 1000
	}
	if position.ExitReason == model.ExitTakeProfit {
		return fmt.Sprintf("✅ TP: %s +%s%% | PnL: %s USDT | Duration: %ds", position.Symbol, position.PnLPercent.StringFixed(4), pnlUSDT(position).StringFixed(4), duration)
	}
	if position.ExitReason == model.ExitStopLoss {
		return fmt.Sprintf("🛑 SL: %s %s%%", position.Symbol, position.PnLPercent.StringFixed(4))
	}
	return fmt.Sprintf("closed: %s reason=%s pnl=%s%% duration=%ds", position.Symbol, position.ExitReason, position.PnLPercent.StringFixed(4), duration)
}

func pnlUSDT(position model.Position) decimal.Decimal {
	return position.EntryPrice.Mul(position.Quantity).Mul(position.PnLPercent).Div(decimal.NewFromInt(100))
}
