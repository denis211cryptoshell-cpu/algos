package exchange

import (
	"context"
	"fmt"

	"algos/internal/model"

	"github.com/shopspring/decimal"
)

// Stub is a no-op adapter for exchanges outside the MVP scope.
type Stub struct {
	ExchangeName string
}

func (s Stub) Name() string { return s.ExchangeName }

func (s Stub) ConnectPublic(context.Context, []string, MarketDataHandler) error { return nil }

func (s Stub) PlaceOrder(context.Context, *model.Position) (string, error) { return "", nil }

func (s Stub) CancelOrder(context.Context, string, string) error { return nil }

func (s Stub) GetBalance(context.Context, string) (decimal.Decimal, error) { return decimal.Zero, nil }

func (s Stub) SetLeverage(context.Context, string, int) error { return nil }

func (s Stub) GetMarkPrice(context.Context, string) (decimal.Decimal, error) {
	return decimal.Zero, fmt.Errorf("%s mark price is not implemented", s.ExchangeName)
}

func (s Stub) ClosePosition(context.Context, string, model.Side) error { return nil }

func (s Stub) ListSymbols(context.Context) ([]string, error) { return nil, nil }

func (s Stub) SubscribeSymbols([]string) error { return nil }
