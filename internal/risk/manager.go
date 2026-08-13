package risk

import (
	"sync"
	"time"

	"algos/internal/config"
	"algos/internal/model"

	"github.com/shopspring/decimal"
)

type Manager struct {
	cfg               config.BotConfig
	mu                sync.Mutex
	consecutiveLosses int
	killSwitch        bool
}

func NewManager(cfg config.BotConfig) *Manager {
	return &Manager{cfg: cfg}
}

func (m *Manager) CanTrade() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.killSwitch
}

func (m *Manager) KillSwitchActive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.killSwitch
}

func (m *Manager) ResetKillSwitch() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.killSwitch = false
	m.consecutiveLosses = 0
}

func (m *Manager) EvaluateExit(position model.Position, markPrice decimal.Decimal, now time.Time) (bool, model.ExitReason, decimal.Decimal) {
	pnl := PnLPercent(position, markPrice)
	if position.Side == model.SideLong || position.Side == model.SideShort {
		if pnl.GreaterThanOrEqual(m.cfg.TPPercent) {
			return true, model.ExitTakeProfit, pnl
		}
		if pnl.LessThanOrEqual(m.cfg.SLPercent.Neg()) {
			return true, model.ExitStopLoss, pnl
		}
	}
	if !position.OpenedAt.IsZero() && now.Sub(position.OpenedAt) >= time.Duration(m.cfg.MaxPositionSeconds)*time.Second {
		return true, model.ExitMaxLifetime, pnl
	}
	return false, model.ExitNone, pnl
}

func (m *Manager) RegisterClosedPosition(position model.Position) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if position.PnLPercent.IsNegative() {
		m.consecutiveLosses++
	} else {
		m.consecutiveLosses = 0
	}
	if m.consecutiveLosses >= m.cfg.KillSwitchLosses {
		m.killSwitch = true
	}
}

func PnLPercent(position model.Position, markPrice decimal.Decimal) decimal.Decimal {
	if !position.EntryPrice.IsPositive() || !markPrice.IsPositive() {
		return decimal.Zero
	}
	move := markPrice.Sub(position.EntryPrice).Div(position.EntryPrice).Mul(decimal.NewFromInt(100))
	if position.Side == model.SideShort {
		move = move.Neg()
	}
	return move
}