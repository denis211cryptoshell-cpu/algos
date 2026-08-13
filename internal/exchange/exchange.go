package exchange

import (
	"context"

	"algos/internal/model"

	"github.com/shopspring/decimal"
)

type MarketDataHandler interface {
	OnTrade(model.Trade)
	OnOrderbook(model.Orderbook)
}

type Client interface {
	Name() string
	ConnectPublic(ctx context.Context, symbols []string, handler MarketDataHandler) error
	PlaceOrder(ctx context.Context, pos *model.Position) (string, error)
	CancelOrder(ctx context.Context, symbol, orderID string) error
	GetBalance(ctx context.Context, asset string) (decimal.Decimal, error)
	SetLeverage(ctx context.Context, symbol string, leverage int) error
	GetMarkPrice(ctx context.Context, symbol string) (decimal.Decimal, error)
	ClosePosition(ctx context.Context, symbol string, side model.Side) error
	ListSymbols(ctx context.Context) ([]string, error)
	SubscribeSymbols(symbols []string) error
}
