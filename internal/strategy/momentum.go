package strategy

import (
	"fmt"
	"time"

	"algos/internal/config"
	"algos/internal/model"

	"github.com/shopspring/decimal"
)

var (
	decOne  = decimal.NewFromInt(1)
	decTwo  = decimal.NewFromInt(2)
	dec100  = decimal.NewFromInt(100)
	decHalf = decOne.Div(decTwo)
)

type MomentumStrategy struct {
	cfg config.StrategyConfig
}

func NewMomentumStrategy(cfg config.StrategyConfig) *MomentumStrategy {
	return &MomentumStrategy{cfg: cfg}
}

func (s *MomentumStrategy) Evaluate(snapshot model.MarketSnapshot) model.Signal {
	now := snapshot.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	base := model.Signal{
		Action:     model.SignalHold,
		Exchange:   snapshot.Exchange,
		Symbol:     snapshot.Symbol,
		EntryPrice: snapshot.LastPrice,
		CreatedAt:  now,
	}

	if snapshot.ListingTime.IsZero() || now.Sub(snapshot.ListingTime) > time.Duration(s.cfg.OpeningWindowSeconds)*time.Second {
		base.Reason = "outside opening window"
		return base
	}
	if !snapshot.LastPrice.IsPositive() {
		base.Reason = "last price is not positive"
		return base
	}
	if s.cfg.MinTrades > 0 && len(snapshot.Trades) < s.cfg.MinTrades {
		base.Reason = "not enough trades"
		return base
	}
	if s.cfg.MaxSpreadPercent.IsPositive() && spreadPercent(snapshot.Orderbook).GreaterThan(s.cfg.MaxSpreadPercent) {
		base.Reason = "spread is too wide"
		return base
	}

	buyNotional, sellNotional := tradeNotionals(snapshot.Trades, snapshot.ListingTime, now)
	bidNotional, askNotional := bookNotionals(snapshot.Orderbook, snapshot.LastPrice, s.cfg.OrderbookRadiusPercent)
	liquidAskNotional := askLiquidity(snapshot.Orderbook, snapshot.LastPrice, s.cfg.LiquidityRadiusPercent)
	liquidBidNotional := bidLiquidity(snapshot.Orderbook, snapshot.LastPrice, s.cfg.LiquidityRadiusPercent)

	base.BuyNotional = buyNotional
	base.SellNotional = sellNotional
	base.BidNotional = bidNotional
	base.AskNotional = askNotional

	longTradeOK := ratioAtLeast(buyNotional, sellNotional, s.cfg.TradeVelocityRatio)
	longBookOK := ratioAtLeast(bidNotional, askNotional, s.cfg.ImbalanceRatio)
	longLiquidityOK := !s.cfg.MinLiquidityNotional.IsPositive() || liquidAskNotional.GreaterThanOrEqual(s.cfg.MinLiquidityNotional)
	longFairOK := s.longFairPriceOK(snapshot.OpeningPrice)
	if longTradeOK && longBookOK && longFairOK && longLiquidityOK {
		qty := quantityFromLiquidity(liquidAskNotional, snapshot.LastPrice, s.cfg.MaxPositionPctLiquidity)
		base.Action = model.SignalEnterLong
		base.Quantity = qty
		base.Reason = fmt.Sprintf("long momentum: buy/sell=%s, bid/ask=%s", safeRatioString(buyNotional, sellNotional), safeRatioString(bidNotional, askNotional))
		return base
	}

	shortTradeOK := ratioAtLeast(sellNotional, buyNotional, s.cfg.TradeVelocityRatio)
	shortBookOK := ratioAtLeast(askNotional, bidNotional, s.cfg.ImbalanceRatio)
	shortLiquidityOK := !s.cfg.MinLiquidityNotional.IsPositive() || liquidBidNotional.GreaterThanOrEqual(s.cfg.MinLiquidityNotional)
	shortFairOK := s.shortFairPriceOK(snapshot.OpeningPrice)
	if shortTradeOK && shortBookOK && shortFairOK && shortLiquidityOK {
		qty := quantityFromLiquidity(liquidBidNotional, snapshot.LastPrice, s.cfg.MaxPositionPctLiquidity)
		base.Action = model.SignalEnterShort
		base.Quantity = qty
		base.Reason = fmt.Sprintf("short momentum: sell/buy=%s, ask/bid=%s", safeRatioString(sellNotional, buyNotional), safeRatioString(askNotional, bidNotional))
		return base
	}

	base.Reason = fmt.Sprintf("no setup: buy=%s sell=%s bid=%s ask=%s", buyNotional, sellNotional, bidNotional, askNotional)
	return base
}

func spreadPercent(book model.Orderbook) decimal.Decimal {
	if len(book.Bids) == 0 || len(book.Asks) == 0 || !book.Bids[0].Price.IsPositive() || !book.Asks[0].Price.IsPositive() {
		return decimal.Zero
	}
	mid := book.Bids[0].Price.Add(book.Asks[0].Price).Div(decTwo)
	return book.Asks[0].Price.Sub(book.Bids[0].Price).Div(mid).Mul(dec100)
}

func (s *MomentumStrategy) longFairPriceOK(openingPrice decimal.Decimal) bool {
	if !s.cfg.DEXFairPriceEnabled {
		return true
	}
	return openingPrice.LessThanOrEqual(s.cfg.DEXFairPrice)
}

func (s *MomentumStrategy) shortFairPriceOK(openingPrice decimal.Decimal) bool {
	if !s.cfg.DEXFairPriceEnabled {
		return true
	}
	threshold := s.cfg.DEXFairPrice.Mul(s.cfg.ShortOverpriceMultiplier)
	return openingPrice.GreaterThanOrEqual(threshold)
}

func tradeNotionals(trades []model.Trade, from, to time.Time) (decimal.Decimal, decimal.Decimal) {
	buy := decimal.Zero
	sell := decimal.Zero
	for _, trade := range trades {
		if trade.Timestamp.Before(from) || trade.Timestamp.After(to) {
			continue
		}
		switch trade.Side {
		case model.SideBuy:
			buy = buy.Add(trade.Notional())
		case model.SideSell:
			sell = sell.Add(trade.Notional())
		}
	}
	return buy, sell
}

func bookNotionals(book model.Orderbook, mid decimal.Decimal, radiusPct decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
	return bidLiquidity(book, mid, radiusPct), askLiquidity(book, mid, radiusPct)
}

func bidLiquidity(book model.Orderbook, mid decimal.Decimal, radiusPct decimal.Decimal) decimal.Decimal {
	if !mid.IsPositive() {
		return decimal.Zero
	}
	lower := mid.Mul(decOne.Sub(radiusPct.Div(dec100)))
	total := decimal.Zero
	for _, level := range book.Bids {
		if level.Price.GreaterThanOrEqual(lower) && level.Price.LessThanOrEqual(mid) {
			total = total.Add(level.Notional())
		}
	}
	return total
}

func askLiquidity(book model.Orderbook, mid decimal.Decimal, radiusPct decimal.Decimal) decimal.Decimal {
	if !mid.IsPositive() {
		return decimal.Zero
	}
	upper := mid.Mul(decOne.Add(radiusPct.Div(dec100)))
	total := decimal.Zero
	for _, level := range book.Asks {
		if level.Price.GreaterThanOrEqual(mid) && level.Price.LessThanOrEqual(upper) {
			total = total.Add(level.Notional())
		}
	}
	return total
}

func quantityFromLiquidity(liquidityNotional, price, pct decimal.Decimal) decimal.Decimal {
	if !liquidityNotional.IsPositive() || !price.IsPositive() || !pct.IsPositive() {
		return decimal.Zero
	}
	positionNotional := liquidityNotional.Mul(pct).Div(dec100)
	return positionNotional.Div(price)
}

func ratioAtLeast(numerator, denominator, minRatio decimal.Decimal) bool {
	if !numerator.IsPositive() {
		return false
	}
	if denominator.IsZero() {
		return true
	}
	return numerator.Div(denominator).GreaterThanOrEqual(minRatio)
}

func safeRatioString(numerator, denominator decimal.Decimal) string {
	if denominator.IsZero() {
		return "inf"
	}
	return numerator.Div(denominator).StringFixed(4)
}
