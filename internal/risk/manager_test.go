package risk

import (
	"testing"
	"time"

	"algos/internal/config"
	"algos/internal/model"

	"github.com/shopspring/decimal"
)

func TestEvaluateExitLongTakeProfitStopLossAndLifetime(t *testing.T) {
	m := NewManager(config.BotConfig{MaxPositionSeconds: 20, KillSwitchLosses: 3, MaxLeverage: 3, TPPercent: rd("4"), SLPercent: rd("1.5")})
	openedAt := time.Now().UTC()
	position := model.Position{Side: model.SideLong, EntryPrice: rd("100"), OpenedAt: openedAt}

	shouldExit, reason, pnl := m.EvaluateExit(position, rd("104"), openedAt.Add(time.Second))
	if !shouldExit || reason != model.ExitTakeProfit || !pnl.Equal(rd("4")) {
		t.Fatalf("expected TP 4%%, got exit=%v reason=%s pnl=%s", shouldExit, reason, pnl)
	}

	shouldExit, reason, pnl = m.EvaluateExit(position, rd("98.5"), openedAt.Add(time.Second))
	if !shouldExit || reason != model.ExitStopLoss || !pnl.Equal(rd("-1.500")) {
		t.Fatalf("expected SL -1.5%%, got exit=%v reason=%s pnl=%s", shouldExit, reason, pnl)
	}

	shouldExit, reason, _ = m.EvaluateExit(position, rd("100.1"), openedAt.Add(20*time.Second))
	if !shouldExit || reason != model.ExitMaxLifetime {
		t.Fatalf("expected max lifetime, got exit=%v reason=%s", shouldExit, reason)
	}
}

func TestKillSwitchAfterConsecutiveLosses(t *testing.T) {
	m := NewManager(config.BotConfig{KillSwitchLosses: 3})
	for i := 0; i < 3; i++ {
		m.RegisterClosedPosition(model.Position{PnLPercent: rd("-0.1")})
	}
	if !m.KillSwitchActive() || m.CanTrade() {
		t.Fatal("expected active kill switch after 3 losses")
	}
	m.ResetKillSwitch()
	if m.KillSwitchActive() || !m.CanTrade() {
		t.Fatal("expected reset kill switch")
	}
}

func rd(value string) decimal.Decimal {
	parsed, err := decimal.NewFromString(value)
	if err != nil {
		panic(err)
	}
	return parsed
}
