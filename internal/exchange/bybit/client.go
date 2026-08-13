package bybit

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/bytedance/sonic"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"algos/internal/config"
	"algos/internal/exchange"
	"algos/internal/model"

	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type Client struct {
	cfg    config.ExchangeConfig
	bot    config.BotConfig
	resty  *resty.Client
	logger *zap.Logger

	bookMu sync.Mutex
	books  map[string]*localBook

	instrumentMu sync.Mutex
	instruments  map[string]instrumentRule

	connMu sync.Mutex
	conn   *websocket.Conn

	timeOffset int64 // offset from server time in milliseconds
}

func New(cfg config.ExchangeConfig, logger *zap.Logger, botCfg ...config.BotConfig) *Client {
	bot := config.BotConfig{RecvWindow: "5000"}
	if len(botCfg) > 0 {
		bot = botCfg[0]
	}
	if bot.RecvWindow == "" {
		bot.RecvWindow = "5000"
	}
	c := &Client{
		cfg:         cfg,
		bot:         bot,
		resty:       resty.New().SetBaseURL(cfg.BaseURL).SetTimeout(10 * time.Second),
		logger:      logger.Named("bybit"),
		books:       make(map[string]*localBook),
		instruments: make(map[string]instrumentRule),
	}
	c.syncTime()
	return c
}

func (c *Client) syncTime() {
	resp, err := c.resty.R().Get("/v5/market/time")
	if err != nil {
		c.logger.Warn("failed to sync time with bybit server", zap.Error(err))
		return
	}
	var result struct {
		Result struct {
			TimeNano string `json:"timeNano"`
		} `json:"result"`
	}
	if err := sonic.Unmarshal(resp.Body(), &result); err != nil {
		c.logger.Warn("failed to parse bybit server time", zap.Error(err))
		return
	}
	nano, err := strconv.ParseInt(result.Result.TimeNano, 10, 64)
	if err != nil {
		c.logger.Warn("failed to parse timeNano value", zap.Error(err))
		return
	}
	serverMs := nano / 1_000_000
	localMs := time.Now().UnixMilli()
	c.timeOffset = serverMs - localMs
	c.logger.Info("time synced with bybit server", zap.Int64("offset_ms", c.timeOffset))
}

func (c *Client) nowMs() int64 {
	return time.Now().UnixMilli() + c.timeOffset
}

func (c *Client) Name() string { return "bybit" }

func (c *Client) ConnectPublic(ctx context.Context, symbols []string, handler exchange.MarketDataHandler) error {
	if c.cfg.WSPublic == "" {
		return fmt.Errorf("bybit ws_public is empty")
	}
	backoff := time.Second
	for {
		if err := c.connectPublicOnce(ctx, symbols, handler); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Warn("bybit public ws disconnected", zap.Error(err), zap.Duration("backoff", backoff))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			continue
		}
		backoff = time.Second
	}
}

func (c *Client) connectPublicOnce(ctx context.Context, symbols []string, handler exchange.MarketDataHandler) error {
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, c.cfg.WSPublic, http.Header{})
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial bybit public ws status=%s: %w", resp.Status, err)
		}
		return fmt.Errorf("dial bybit public ws: %w", err)
	}
	defer conn.Close()

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	args := make([]string, 0, len(symbols)*2)
	for _, symbol := range symbols {
		args = append(args, "publicTrade."+symbol, "orderbook.200."+symbol)
	}
	if err := conn.WriteJSON(map[string]any{"op": "subscribe", "args": args}); err != nil {
		return fmt.Errorf("subscribe bybit public ws: %w", err)
	}
	c.logger.Info("subscribed bybit public ws", zap.Strings("args", args))

	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := conn.WriteJSON(map[string]string{"op": "ping"}); err != nil {
					c.logger.Warn("bybit ws ping failed", zap.Error(err))
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, raw, err := conn.ReadMessage()
		if err != nil {
			<-pingDone
			return fmt.Errorf("read bybit public ws: %w", err)
		}
		c.handlePublicMessage(raw, handler)
	}
}

func (c *Client) PlaceOrder(ctx context.Context, pos *model.Position) (string, error) {
	if pos == nil {
		return "", fmt.Errorf("position is nil")
	}
	rule, err := c.getInstrumentRule(ctx, pos.Symbol)
	if err != nil {
		return "", err
	}
	qty := rule.roundQty(pos.Quantity)
	if qty.IsZero() || qty.IsNegative() {
		return "", fmt.Errorf("rounded quantity is not positive for %s", pos.Symbol)
	}
	if rule.MinOrderQty.IsPositive() && qty.LessThan(rule.MinOrderQty) {
		return "", fmt.Errorf("quantity %s is less than minOrderQty %s for %s", qty, rule.MinOrderQty, pos.Symbol)
	}
	side := bybitOrderSide(pos.Side, false)
	body := map[string]any{
		"category":    "linear",
		"symbol":      pos.Symbol,
		"side":        side,
		"orderType":   "Market",
		"qty":         qty.String(),
		"timeInForce": "IOC",
	}
	var result bybitOrderCreateResult
	if err := c.privateJSON(ctx, http.MethodPost, "/v5/order/create", nil, body, &result); err != nil {
		return "", err
	}
	return result.OrderID, nil
}

func (c *Client) CancelOrder(ctx context.Context, symbol, orderID string) error {
	body := map[string]any{"category": "linear", "symbol": symbol, "orderId": orderID}
	return c.privateJSON(ctx, http.MethodPost, "/v5/order/cancel", nil, body, nil)
}

func (c *Client) GetBalance(ctx context.Context, asset string) (decimal.Decimal, error) {
	query := map[string]string{"accountType": "UNIFIED"}
	var result bybitWalletResult
	if err := c.privateJSON(ctx, http.MethodGet, "/v5/account/wallet-balance", query, nil, &result); err != nil {
		return decimal.Zero, err
	}
	for _, account := range result.List {
		for _, coin := range account.Coin {
			if coin.Coin == asset {
				value, err := decimal.NewFromString(coin.WalletBalance)
				if err != nil {
					return decimal.Zero, fmt.Errorf("parse bybit balance: %w", err)
				}
				return value, nil
			}
		}
	}
	return decimal.Zero, nil
}

func (c *Client) SetLeverage(ctx context.Context, symbol string, leverage int) error {
	lev := strconv.Itoa(leverage)
	body := map[string]any{"category": "linear", "symbol": symbol, "buyLeverage": lev, "sellLeverage": lev}
	return c.privateJSON(ctx, http.MethodPost, "/v5/position/set-leverage", nil, body, nil)
}

func (c *Client) GetMarkPrice(ctx context.Context, symbol string) (decimal.Decimal, error) {
	query := map[string]string{"category": "linear", "symbol": symbol}
	var result bybitTickersResult
	if err := c.publicGET(ctx, "/v5/market/tickers", query, &result); err != nil {
		return decimal.Zero, err
	}
	if len(result.List) == 0 {
		return decimal.Zero, fmt.Errorf("bybit ticker not found: %s", symbol)
	}
	price, err := decimal.NewFromString(result.List[0].MarkPrice)
	if err != nil {
		return decimal.Zero, fmt.Errorf("parse bybit mark price: %w", err)
	}
	return price, nil
}

func (c *Client) ClosePosition(ctx context.Context, symbol string, side model.Side) error {
	position, err := c.GetPosition(ctx, symbol, side)
	if err != nil {
		return err
	}
	if !position.Size.IsPositive() {
		return fmt.Errorf("no open position to close for %s %s", symbol, side)
	}
	orderSide := bybitOrderSide(side, true)
	body := map[string]any{
		"category":       "linear",
		"symbol":         symbol,
		"side":           orderSide,
		"orderType":      "Market",
		"qty":            position.Size.String(),
		"reduceOnly":     true,
		"closeOnTrigger": true,
	}
	return c.privateJSON(ctx, http.MethodPost, "/v5/order/create", nil, body, nil)
}

type PositionInfo struct {
	Symbol      string
	Side        model.Side
	Size        decimal.Decimal
	AvgPrice    decimal.Decimal
	PositionIdx int
	RawSide     string
}

func (c *Client) GetPosition(ctx context.Context, symbol string, side model.Side) (PositionInfo, error) {
	query := map[string]string{"category": "linear", "symbol": symbol}
	var result bybitPositionListResult
	if err := c.privateJSON(ctx, http.MethodGet, "/v5/position/list", query, nil, &result); err != nil {
		return PositionInfo{}, err
	}
	wantSide := "Buy"
	if side == model.SideShort || side == model.SideSell {
		wantSide = "Sell"
	}
	for _, item := range result.List {
		if item.Symbol != symbol || item.Side != wantSide {
			continue
		}
		size, err := decimal.NewFromString(item.Size)
		if err != nil {
			return PositionInfo{}, fmt.Errorf("parse bybit position size: %w", err)
		}
		avgPrice := decimal.Zero
		if item.AvgPrice != "" {
			parsed, err := decimal.NewFromString(item.AvgPrice)
			if err != nil {
				return PositionInfo{}, fmt.Errorf("parse bybit avg price: %w", err)
			}
			avgPrice = parsed
		}
		return PositionInfo{Symbol: item.Symbol, Side: side, Size: size, AvgPrice: avgPrice, PositionIdx: item.PositionIdx, RawSide: item.Side}, nil
	}
	return PositionInfo{Symbol: symbol, Side: side}, nil
}

type publicEnvelope struct {
	Topic string          `json:"topic"`
	Type  string          `json:"type"`
	TS    int64           `json:"ts"`
	Data  json.RawMessage `json:"data"`
}

type bybitTrade struct {
	Symbol string `json:"s"`
	Side   string `json:"S"`
	Price  string `json:"p"`
	Size   string `json:"v"`
	TS     int64  `json:"T"`
}

type bybitOrderbook struct {
	Symbol string     `json:"s"`
	Bids   [][]string `json:"b"`
	Asks   [][]string `json:"a"`
	Update int64      `json:"u"`
	Seq    int64      `json:"seq"`
}

func (c *Client) handlePublicMessage(raw []byte, handler exchange.MarketDataHandler) {
	var env publicEnvelope
	if err := sonic.Unmarshal(raw, &env); err != nil {
		c.logger.Debug("skip invalid bybit message", zap.Error(err))
		return
	}
	if env.Topic == "" || len(env.Data) == 0 {
		return
	}
	if hasPrefix(env.Topic, "publicTrade.") {
		var trades []bybitTrade
		if err := sonic.Unmarshal(env.Data, &trades); err != nil {
			c.logger.Debug("skip invalid bybit trade", zap.Error(err))
			return
		}
		for _, tr := range trades {
			trade, err := convertTrade(tr)
			if err != nil {
				c.logger.Debug("skip bybit trade conversion", zap.Error(err))
				continue
			}
			handler.OnTrade(trade)
		}
		return
	}
	if hasPrefix(env.Topic, "orderbook.") {
		var ob bybitOrderbook
		if err := sonic.Unmarshal(env.Data, &ob); err != nil {
			c.logger.Debug("skip invalid bybit orderbook", zap.Error(err))
			return
		}
		snapshot := c.mergeOrderbook(env.Type, ob, env.TS)
		if len(snapshot.Bids) > 0 && len(snapshot.Asks) > 0 {
			handler.OnOrderbook(snapshot)
		}
	}
}

func convertTrade(tr bybitTrade) (model.Trade, error) {
	price, err := decimal.NewFromString(tr.Price)
	if err != nil {
		return model.Trade{}, err
	}
	qty, err := decimal.NewFromString(tr.Size)
	if err != nil {
		return model.Trade{}, err
	}
	side := model.SideSell
	if tr.Side == "Buy" {
		side = model.SideBuy
	}
	return model.Trade{Exchange: "bybit", Symbol: tr.Symbol, Side: side, Price: price, Quantity: qty, Timestamp: time.UnixMilli(tr.TS).UTC()}, nil
}

func convertOrderbook(ob bybitOrderbook, ts int64) model.Orderbook {
	return model.Orderbook{Exchange: "bybit", Symbol: ob.Symbol, Bids: convertLevels(ob.Bids), Asks: convertLevels(ob.Asks), Timestamp: time.UnixMilli(ts).UTC()}
}

func convertLevels(raw [][]string) []model.OrderbookLevel {
	levels := make([]model.OrderbookLevel, 0, len(raw))
	for _, pair := range raw {
		if len(pair) < 2 {
			continue
		}
		price, errP := decimal.NewFromString(pair[0])
		qty, errQ := decimal.NewFromString(pair[1])
		if errP != nil || errQ != nil {
			continue
		}
		levels = append(levels, model.OrderbookLevel{Price: price, Quantity: qty})
	}
	return levels
}

type localBook struct {
	bids map[string]decimal.Decimal
	asks map[string]decimal.Decimal
}

type instrumentRule struct {
	Symbol      string
	QtyStep     decimal.Decimal
	MinOrderQty decimal.Decimal
	MaxOrderQty decimal.Decimal
	TickSize    decimal.Decimal
}

func (r instrumentRule) roundQty(qty decimal.Decimal) decimal.Decimal {
	if !r.QtyStep.IsPositive() {
		return qty
	}
	steps := qty.Div(r.QtyStep).Floor()
	return steps.Mul(r.QtyStep)
}

func (c *Client) getInstrumentRule(ctx context.Context, symbol string) (instrumentRule, error) {
	c.instrumentMu.Lock()
	rule, ok := c.instruments[symbol]
	c.instrumentMu.Unlock()
	if ok {
		return rule, nil
	}
	query := map[string]string{"category": "linear", "symbol": symbol}
	var result bybitInstrumentsResult
	if err := c.publicGET(ctx, "/v5/market/instruments-info", query, &result); err != nil {
		return instrumentRule{}, err
	}
	if len(result.List) == 0 {
		return instrumentRule{}, fmt.Errorf("bybit instrument not found: %s", symbol)
	}
	item := result.List[0]
	rule = instrumentRule{Symbol: item.Symbol}
	var err error
	if rule.QtyStep, err = decimal.NewFromString(item.LotSizeFilter.QtyStep); err != nil {
		return instrumentRule{}, fmt.Errorf("parse qtyStep: %w", err)
	}
	if rule.MinOrderQty, err = decimal.NewFromString(item.LotSizeFilter.MinOrderQty); err != nil {
		return instrumentRule{}, fmt.Errorf("parse minOrderQty: %w", err)
	}
	if item.LotSizeFilter.MaxOrderQty != "" {
		if rule.MaxOrderQty, err = decimal.NewFromString(item.LotSizeFilter.MaxOrderQty); err != nil {
			return instrumentRule{}, fmt.Errorf("parse maxOrderQty: %w", err)
		}
	}
	if item.PriceFilter.TickSize != "" {
		if rule.TickSize, err = decimal.NewFromString(item.PriceFilter.TickSize); err != nil {
			return instrumentRule{}, fmt.Errorf("parse tickSize: %w", err)
		}
	}
	c.instrumentMu.Lock()
	c.instruments[symbol] = rule
	c.instrumentMu.Unlock()
	return rule, nil
}

func (c *Client) mergeOrderbook(messageType string, ob bybitOrderbook, ts int64) model.Orderbook {
	c.bookMu.Lock()
	defer c.bookMu.Unlock()
	book := c.books[ob.Symbol]
	if book == nil {
		book = &localBook{bids: make(map[string]decimal.Decimal, 200), asks: make(map[string]decimal.Decimal, 200)}
		c.books[ob.Symbol] = book
	} else if messageType == "snapshot" {
		for k := range book.bids {
			delete(book.bids, k)
		}
		for k := range book.asks {
			delete(book.asks, k)
		}
	}
	applyLevels(book.bids, ob.Bids)
	applyLevels(book.asks, ob.Asks)
	return model.Orderbook{Exchange: "bybit", Symbol: ob.Symbol, Bids: sortedLevels(book.bids, true, 200), Asks: sortedLevels(book.asks, false, 200), Timestamp: time.UnixMilli(ts).UTC()}
}

func applyLevels(dst map[string]decimal.Decimal, raw [][]string) {
	for _, pair := range raw {
		if len(pair) < 2 {
			continue
		}
		qty, err := decimal.NewFromString(pair[1])
		if err != nil {
			continue
		}
		if qty.IsZero() {
			delete(dst, pair[0])
			continue
		}
		dst[pair[0]] = qty
	}
}

func sortedLevels(src map[string]decimal.Decimal, desc bool, limit int) []model.OrderbookLevel {
	levels := make([]model.OrderbookLevel, 0, len(src))
	for priceText, qty := range src {
		price, err := decimal.NewFromString(priceText)
		if err != nil {
			continue
		}
		levels = append(levels, model.OrderbookLevel{Price: price, Quantity: qty})
	}
	sort.Slice(levels, func(i, j int) bool {
		if desc {
			return levels[i].Price.GreaterThan(levels[j].Price)
		}
		return levels[i].Price.LessThan(levels[j].Price)
	})
	if len(levels) > limit {
		return levels[:limit]
	}
	return levels
}

type bybitAPIResponse struct {
	RetCode int             `json:"retCode"`
	RetMsg  string          `json:"retMsg"`
	Result  json.RawMessage `json:"result"`
}

type bybitOrderCreateResult struct {
	OrderID string `json:"orderId"`
}
type bybitTickersResult struct {
	List []struct {
		MarkPrice string `json:"markPrice"`
	} `json:"list"`
}
type bybitWalletResult struct {
	List []struct {
		Coin []struct {
			Coin          string `json:"coin"`
			WalletBalance string `json:"walletBalance"`
		} `json:"coin"`
	} `json:"list"`
}

type bybitInstrumentsResult struct {
	List []struct {
		Symbol        string `json:"symbol"`
		LotSizeFilter struct {
			MinOrderQty string `json:"minOrderQty"`
			MaxOrderQty string `json:"maxOrderQty"`
			QtyStep     string `json:"qtyStep"`
		} `json:"lotSizeFilter"`
		PriceFilter struct {
			TickSize string `json:"tickSize"`
		} `json:"priceFilter"`
	} `json:"list"`
}

type bybitPositionListResult struct {
	List []struct {
		Symbol      string `json:"symbol"`
		Side        string `json:"side"`
		Size        string `json:"size"`
		AvgPrice    string `json:"avgPrice"`
		PositionIdx int    `json:"positionIdx"`
	} `json:"list"`
}

func (c *Client) publicGET(ctx context.Context, path string, query map[string]string, out any) error {
	resp, err := c.resty.R().SetContext(ctx).SetQueryParams(query).Get(path)
	if err != nil {
		return fmt.Errorf("bybit public get %s: %w", path, err)
	}
	return decodeBybitResponse(resp, out)
}

func (c *Client) SubscribeSymbols(symbols []string) error {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return fmt.Errorf("ws not connected")
	}
	args := make([]string, 0, len(symbols)*2)
	for _, symbol := range symbols {
		args = append(args, "publicTrade."+symbol, "orderbook.200."+symbol)
	}
	if err := conn.WriteJSON(map[string]any{"op": "subscribe", "args": args}); err != nil {
		return fmt.Errorf("subscribe new symbols: %w", err)
	}
	c.logger.Info("subscribed new symbols", zap.Strings("args", args))
	return nil
}

type bybitInstrument struct {
	Symbol string `json:"symbol"`
}

func (c *Client) ListSymbols(ctx context.Context) ([]string, error) {
	var result struct {
		List []bybitInstrument `json:"list"`
	}
	if err := c.publicGET(ctx, "/v5/market/instruments-info", map[string]string{"category": "linear", "status": "Trading"}, &result); err != nil {
		return nil, err
	}
	symbols := make([]string, 0, len(result.List))
	for _, inst := range result.List {
		if strings.HasSuffix(inst.Symbol, "USDT") {
			symbols = append(symbols, inst.Symbol)
		}
	}
	c.logger.Info("listSymbols", zap.Int("total", len(result.List)), zap.Int("usdt", len(symbols)))
	return symbols, nil
}

func (c *Client) privateJSON(ctx context.Context, method, path string, query map[string]string, body any, out any) error {
	if c.cfg.APIKey == "" || c.cfg.APISecret == "" {
		return fmt.Errorf("bybit api credentials are empty")
	}
	bodyBytes := []byte{}
	if body != nil {
		var err error
		bodyBytes, err = sonic.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal bybit request: %w", err)
		}
	}
	queryString := canonicalQuery(query)
	timestamp := strconv.FormatInt(c.nowMs(), 10)
	signPayload := timestamp + c.cfg.APIKey + c.bot.RecvWindow
	if method == http.MethodGet {
		signPayload += queryString
	} else {
		signPayload += string(bodyBytes)
	}
	sig := sign(c.cfg.APISecret, signPayload)
	req := c.resty.R().SetContext(ctx).
		SetHeader("X-BAPI-API-KEY", c.cfg.APIKey).
		SetHeader("X-BAPI-TIMESTAMP", timestamp).
		SetHeader("X-BAPI-RECV-WINDOW", c.bot.RecvWindow).
		SetHeader("X-BAPI-SIGN", sig).
		SetHeader("Content-Type", "application/json")
	if queryString != "" {
		req.SetQueryString(queryString)
	}
	if len(bodyBytes) > 0 {
		req.SetBody(bodyBytes)
	}
	var resp *resty.Response
	var err error
	if method == http.MethodGet {
		resp, err = req.Get(path)
	} else {
		resp, err = req.Post(path)
	}
	if err != nil {
		return fmt.Errorf("bybit private %s %s: %w", method, path, err)
	}
	if resp.StatusCode() == http.StatusTooManyRequests {
		return fmt.Errorf("bybit rate limit: retry-after=%s", resp.Header().Get("Retry-After"))
	}
	return decodeBybitResponse(resp, out)
}

func decodeBybitResponse(resp *resty.Response, out any) error {
	var api bybitAPIResponse
	if err := json.Unmarshal(resp.Body(), &api); err != nil {
		return fmt.Errorf("decode bybit response status=%d: %w", resp.StatusCode(), err)
	}
	if api.RetCode == 10004 {
		panic("bybit signature error: " + api.RetMsg)
	}
	if api.RetCode != 0 {
		return fmt.Errorf("bybit api error code=%d msg=%s", api.RetCode, api.RetMsg)
	}
	if out != nil && len(api.Result) > 0 {
		if err := json.Unmarshal(api.Result, out); err != nil {
			return fmt.Errorf("decode bybit result: %w", err)
		}
	}
	return nil
}

func canonicalQuery(query map[string]string) string {
	if len(query) == 0 {
		return ""
	}
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+query[key])
	}
	return strings.Join(parts, "&")
}

func sign(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

func bybitOrderSide(side model.Side, closing bool) string {
	if closing {
		if side == model.SideLong || side == model.SideBuy {
			return "Sell"
		}
		return "Buy"
	}
	if side == model.SideShort || side == model.SideSell {
		return "Sell"
	}
	return "Buy"
}

func hasPrefix(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

func parseInt64(value string) int64 {
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}
