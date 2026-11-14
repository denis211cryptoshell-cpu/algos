package model

import (
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

var (
	snapshotPool  = sync.Pool{New: func() any { return &MarketSnapshot{} }}
	signalPool    = sync.Pool{New: func() any { return &Signal{} }}
	orderbookPool = sync.Pool{New: func() any { return &Orderbook{} }}
)

func AcquireSnapshot() *MarketSnapshot { return snapshotPool.Get().(*MarketSnapshot) }
func ReleaseSnapshot(s *MarketSnapshot) {
	s.Trades = s.Trades[:0]
	s.Orderbooks = s.Orderbooks[:0]
	snapshotPool.Put(s)
}
func AcquireSignal() *Signal        { return signalPool.Get().(*Signal) }
func ReleaseSignal(s *Signal)       { signalPool.Put(s) }
func AcquireOrderbook() *Orderbook  { return orderbookPool.Get().(*Orderbook) }
func ReleaseOrderbook(o *Orderbook) { o.Bids = o.Bids[:0]; o.Asks = o.Asks[:0]; orderbookPool.Put(o) }

type Side string

const (
	SideBuy   Side = "buy"
	SideSell  Side = "sell"
	SideLong  Side = "long"
	SideShort Side = "short"
)

type SignalAction string

const (
	SignalHold       SignalAction = "HOLD"
	SignalEnterLong  SignalAction = "ENTER_LONG"
	SignalEnterShort SignalAction = "ENTER_SHORT"
)

type ExitReason string

const (
	ExitNone        ExitReason = "NONE"
	ExitTakeProfit  ExitReason = "TAKE_PROFIT"
	ExitStopLoss    ExitReason = "STOP_LOSS"
	ExitMaxLifetime ExitReason = "MAX_LIFETIME"
	ExitManual      ExitReason = "MANUAL"
)

type Trade struct {
	Exchange  string          `json:"exchange"`
	Symbol    string          `json:"symbol"`
	Side      Side            `json:"side"`
	Price     decimal.Decimal `json:"price"`
	Quantity  decimal.Decimal `json:"quantity"`
	Timestamp time.Time       `json:"timestamp"`
}

func (t Trade) Notional() decimal.Decimal {
	return t.Price.Mul(t.Quantity)
}

type OrderbookLevel struct {
	Price    decimal.Decimal `json:"price"`
	Quantity decimal.Decimal `json:"quantity"`
}

func (l OrderbookLevel) Notional() decimal.Decimal {
	return l.Price.Mul(l.Quantity)
}

type Orderbook struct {
	Exchange  string           `json:"exchange"`
	Symbol    string           `json:"symbol"`
	Bids      []OrderbookLevel `json:"bids"`
	Asks      []OrderbookLevel `json:"asks"`
	Timestamp time.Time        `json:"timestamp"`
}

type MarketSnapshot struct {
	Exchange     string
	Symbol       string
	OpeningPrice decimal.Decimal
	LastPrice    decimal.Decimal
	ListingTime  time.Time
	Now          time.Time
	Trades       []Trade
	Orderbook    Orderbook
	Orderbooks   []Orderbook
}

type Signal struct {
	Action       SignalAction
	Exchange     string
	Symbol       string
	EntryPrice   decimal.Decimal
	Quantity     decimal.Decimal
	Reason       string
	CreatedAt    time.Time
	BidNotional  decimal.Decimal
	AskNotional  decimal.Decimal
	BuyNotional  decimal.Decimal
	SellNotional decimal.Decimal
}

type Position struct {
	ID           string          `json:"id"`
	Exchange     string          `json:"exchange"`
	Symbol       string          `json:"symbol"`
	Side         Side            `json:"side"`
	EntryPrice   decimal.Decimal `json:"entry_price"`
	Quantity     decimal.Decimal `json:"quantity"`
	Margin       decimal.Decimal `json:"margin"`
	Leverage     int             `json:"leverage"`
	TpPrice      decimal.Decimal `json:"tp_price"`
	SlPrice      decimal.Decimal `json:"sl_price"`
	OpenedAt     time.Time       `json:"opened_at"`
	MaxHoldUntil time.Time       `json:"max_hold_until"`
	ClosedAt     *time.Time      `json:"closed_at,omitempty"`
	ExitPrice    decimal.Decimal `json:"exit_price"`
	ExitReason   ExitReason      `json:"exit_reason"`
	PnLPercent   decimal.Decimal `json:"pnl_percent"`
}

func (p Position) IsOpen() bool {
	return p.ClosedAt == nil
}
