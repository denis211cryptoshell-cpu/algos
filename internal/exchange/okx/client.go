package okx

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"algos/internal/config"
	"algos/internal/exchange"
	"algos/internal/model"

	"github.com/bytedance/sonic"
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

	connMu      sync.Mutex
	conn        *websocket.Conn
	privateConn *websocket.Conn
	privateMu   sync.Mutex
	orderResp   map[string]chan orderResult

	simulated  bool
	timeOffset int64
}

type orderResult struct {
	OrderID string
	Error   error
}

type localBook struct {
	bids map[string]decimal.Decimal
	asks map[string]decimal.Decimal
}

func New(cfg config.ExchangeConfig, logger *zap.Logger, botCfg ...config.BotConfig) *Client {
	bot := config.BotConfig{RecvWindow: "5000"}
	if len(botCfg) > 0 {
		bot = botCfg[0]
	}
	if bot.RecvWindow == "" {
		bot.RecvWindow = "5000"
	}
	// Mode wiring: testnet = OKX demo (simulated trading), mainnet = real trading
	simulated := bot.Mode != "mainnet"
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://www.okx.com"
	}
	if cfg.WSPublic == "" {
		if simulated {
			cfg.WSPublic = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
		} else {
			cfg.WSPublic = "wss://ws.okx.com:8443/ws/v5/public"
		}
	}
	if cfg.WSPrivate == "" {
		if simulated {
			cfg.WSPrivate = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
		} else {
			cfg.WSPrivate = "wss://ws.okx.com:8443/ws/v5/private"
		}
	}
	// Optimized HTTP client with keep-alive and connection pooling
	httpClient := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
		},
	}
	restyClient := resty.NewWithClient(httpClient).SetBaseURL(cfg.BaseURL)
	c := &Client{
		cfg:       cfg,
		bot:       bot,
		resty:     restyClient,
		logger:    logger.Named("okx"),
		books:     make(map[string]*localBook),
		simulated: simulated,
	}
	c.syncTime()
	c.orderResp = make(map[string]chan orderResult)
	go c.connectPrivateWS()
	return c
}

func (c *Client) connectPrivateWS() {
	wsURL := c.cfg.WSPrivate
	if wsURL == "" {
		c.logger.Error("okx private ws url is empty")
		return
	}

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			c.logger.Warn("okx private ws dial failed", zap.Error(err))
			time.Sleep(3 * time.Second)
			continue
		}

		c.privateMu.Lock()
		c.privateConn = conn
		c.privateMu.Unlock()

		// Login
		if err := c.wsLogin(conn); err != nil {
			c.logger.Warn("okx private ws login failed", zap.Error(err))
			conn.Close()
			time.Sleep(3 * time.Second)
			continue
		}
		c.logger.Info("okx private ws connected")

		// Read loop
		go c.readPrivateWS(conn)
		return
	}
}

func (c *Client) wsLogin(conn *websocket.Conn) error {
	timestamp := strconv.FormatInt(c.nowMs()/1000, 10)
	signPayload := timestamp + "GET" + "/users/self/verify"
	mac := hmac.New(sha256.New, []byte(c.cfg.APISecret))
	mac.Write([]byte(signPayload))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	loginMsg := map[string]any{
		"op": "login",
		"args": []map[string]string{{
			"apiKey":     c.cfg.APIKey,
			"passphrase": c.cfg.Passphrase,
			"timestamp":  timestamp,
			"sign":       sig,
		}},
	}
	return conn.WriteJSON(loginMsg)
}

func (c *Client) readPrivateWS(conn *websocket.Conn) {
	defer conn.Close()
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			c.logger.Warn("okx private ws read error", zap.Error(err))
			go c.connectPrivateWS()
			return
		}
		c.handlePrivateMessage(raw)
	}
}

func (c *Client) handlePrivateMessage(raw []byte) {
	var msg struct {
		Op      string                   `json:"op"`
		Arg     struct{ Channel string } `json:"arg"`
		Data    json.RawMessage          `json:"data"`
		Event   string                   `json:"event"`
		Msg     string                   `json:"msg"`
		Code    string                   `json:"code"`
		ReqID   string                   `json:"reqId"`
		ID      string                   `json:"id"`
		ClOrdID string                   `json:"clOrdId"`
		OrdID   string                   `json:"ordId"`
		SCode   string                   `json:"sCode"`
		SMsg    string                   `json:"sMsg"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.Event == "login" {
		c.logger.Info("okx private ws login success")
		return
	}
	if msg.Event == "error" {
		c.logger.Warn("okx private ws error", zap.String("code", msg.Code), zap.String("msg", msg.Msg))
		return
	}
	// Handle order responses - check both channel and top-level fields
	if msg.Arg.Channel == "orders" || (msg.Op == "order" && len(msg.Data) > 0) {
		var orders []struct {
			ClOrdID string `json:"clOrdId"`
			OrdID   string `json:"ordId"`
			SCode   string `json:"sCode"`
			SMsg    string `json:"sMsg"`
		}
		if err := json.Unmarshal(msg.Data, &orders); err == nil {
			for _, o := range orders {
				c.privateMu.Lock()
				ch, ok := c.orderResp[o.ClOrdID]
				c.privateMu.Unlock()
				if ok {
					result := orderResult{OrderID: o.OrdID}
					if o.SCode != "0" {
						result.Error = fmt.Errorf("okx order error: %s %s", o.SCode, o.SMsg)
					}
					// Non-blocking: never stall the private WS read loop
					select {
					case ch <- result:
					default:
					}
				}
			}
		}
	}
}

func (c *Client) syncTime() {
	resp, err := c.resty.R().Get("/api/v5/public/time")
	if err != nil {
		c.logger.Warn("failed to sync time with okx server", zap.Error(err))
		return
	}
	var result struct {
		Data []struct {
			Ts string `json:"ts"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		c.logger.Warn("failed to parse okx server time", zap.Error(err))
		return
	}
	if len(result.Data) == 0 {
		return
	}
	serverMs, err := strconv.ParseInt(result.Data[0].Ts, 10, 64)
	if err != nil {
		c.logger.Warn("failed to parse ts value", zap.Error(err))
		return
	}
	localMs := time.Now().UnixMilli()
	c.timeOffset = serverMs - localMs
	c.logger.Info("time synced with okx server", zap.Int64("offset_ms", c.timeOffset))
}

func (c *Client) nowMs() int64 {
	return time.Now().UnixMilli() + c.timeOffset
}

func (c *Client) ConnectPublic(ctx context.Context, symbols []string, handler exchange.MarketDataHandler) error {
	if c.cfg.WSPublic == "" {
		return fmt.Errorf("okx ws_public is empty")
	}
	backoff := time.Second
	for {
		if err := c.connectPublicOnce(ctx, symbols, handler); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Warn("okx public ws disconnected", zap.Error(err), zap.Duration("backoff", backoff))
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
			return fmt.Errorf("dial okx public ws status=%s: %w", resp.Status, err)
		}
		return fmt.Errorf("dial okx public ws: %w", err)
	}
	defer conn.Close()

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	// Subscribe to tickers and books channels
	args := make([]map[string]string, 0, len(symbols)*2)
	for _, symbol := range symbols {
		args = append(args, map[string]string{"channel": "tickers", "instId": symbol})
		args = append(args, map[string]string{"channel": "books5", "instId": symbol})
	}
	subMsg := map[string]any{"op": "subscribe", "args": args}
	if err := conn.WriteJSON(subMsg); err != nil {
		return fmt.Errorf("subscribe okx public ws: %w", err)
	}
	c.logger.Info("subscribed okx public ws", zap.Int("channels", len(args)))

	// Ping goroutine
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
					c.logger.Warn("okx ws ping failed", zap.Error(err))
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
			return fmt.Errorf("read okx public ws: %w", err)
		}
		// Skip pong responses
		if string(raw) == "pong" {
			continue
		}
		c.handlePublicMessage(raw, handler)
	}
}

func (c *Client) handlePublicMessage(raw []byte, handler exchange.MarketDataHandler) {
	var msg struct {
		Arg struct {
			Channel string `json:"channel"`
			InstID  string `json:"instId"`
		} `json:"arg"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		c.logger.Debug("skip invalid okx message", zap.Error(err))
		return
	}
	if msg.Arg.Channel == "" {
		return
	}

	switch msg.Arg.Channel {
	case "tickers":
		var ticker struct {
			Last string `json:"last"`
			Side string `json:"side"`
			Inst string `json:"instId"`
		}
		if err := json.Unmarshal(msg.Data, &ticker); err == nil && len(msg.Data) > 2 {
			// Wrap as a trade-like event for the engine
			price, _ := decimal.NewFromString(ticker.Last)
			if price.IsPositive() {
				side := model.SideBuy
				if ticker.Side == "sell" {
					side = model.SideSell
				}
				handler.OnTrade(model.Trade{
					Exchange:  "okx",
					Symbol:    ticker.Inst,
					Side:      side,
					Price:     price,
					Quantity:  decimal.NewFromInt(1),
					Timestamp: time.Now().UTC(),
				})
			}
		}
	case "books5":
		var book struct {
			Bids [][]string `json:"bids"`
			Asks [][]string `json:"asks"`
		}
		if err := json.Unmarshal(msg.Data, &book); err == nil {
			snapshot := c.mergeOrderbook(msg.Arg.InstID, book.Bids, book.Asks)
			if len(snapshot.Bids) > 0 && len(snapshot.Asks) > 0 {
				handler.OnOrderbook(snapshot)
			}
		}
	}
}

func (c *Client) mergeOrderbook(symbol string, rawBids, rawAsks [][]string) model.Orderbook {
	c.bookMu.Lock()
	defer c.bookMu.Unlock()
	book := c.books[symbol]
	if book == nil {
		book = &localBook{bids: make(map[string]decimal.Decimal, 5), asks: make(map[string]decimal.Decimal, 5)}
		c.books[symbol] = book
	} else {
		for k := range book.bids {
			delete(book.bids, k)
		}
		for k := range book.asks {
			delete(book.asks, k)
		}
	}
	applyLevels(book.bids, rawBids)
	applyLevels(book.asks, rawAsks)
	return model.Orderbook{
		Exchange:  "okx",
		Symbol:    symbol,
		Bids:      sortedLevels(book.bids, true, 5),
		Asks:      sortedLevels(book.asks, false, 5),
		Timestamp: time.Now().UTC(),
	}
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

func (c *Client) SubscribeSymbols(symbols []string) error {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return fmt.Errorf("ws not connected")
	}
	args := make([]map[string]string, 0, len(symbols)*2)
	for _, symbol := range symbols {
		args = append(args, map[string]string{"channel": "tickers", "instId": symbol})
		args = append(args, map[string]string{"channel": "books5", "instId": symbol})
	}
	subMsg := map[string]any{"op": "subscribe", "args": args}
	if err := conn.WriteJSON(subMsg); err != nil {
		return fmt.Errorf("subscribe new symbols: %w", err)
	}
	c.logger.Info("subscribed new symbols", zap.Int("count", len(args)))
	return nil
}

func (c *Client) ListSymbols(ctx context.Context) ([]string, error) {
	var result struct {
		Data []struct {
			InstID string `json:"instId"`
			State  string `json:"state"`
		} `json:"data"`
	}
	resp, err := c.resty.R().SetContext(ctx).Get("/api/v5/public/instruments?instType=SWAP")
	if err != nil {
		return nil, fmt.Errorf("okx list instruments: %w", err)
	}
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return nil, fmt.Errorf("okx parse instruments: %w", err)
	}
	symbols := make([]string, 0, len(result.Data))
	for _, inst := range result.Data {
		if inst.State == "live" && strings.HasSuffix(inst.InstID, "-USDT-SWAP") {
			symbols = append(symbols, inst.InstID)
		}
	}
	c.logger.Info("listSymbols", zap.Int("total", len(result.Data)), zap.Int("usdt_swaps", len(symbols)))
	return symbols, nil
}

func (c *Client) PlaceOrder(ctx context.Context, pos *model.Position) (string, error) {
	if pos == nil {
		return "", fmt.Errorf("position is nil")
	}
	side := "buy"
	posSide := "long"
	if pos.Side == model.SideShort || pos.Side == model.SideSell {
		side = "sell"
		posSide = "short"
	}

	// Use REST (WS orders not supported on demo)
	return c.placeOrderREST(ctx, pos.Symbol, side, posSide, pos.Quantity.String())
}

func (c *Client) placeOrderWS(conn *websocket.Conn, symbol, side, posSide, sz string) (string, error) {
	clientOrdID := fmt.Sprintf("bot%d", time.Now().UnixNano())
	reqID := fmt.Sprintf("%d", time.Now().UnixNano())
	ch := make(chan orderResult, 1)

	c.privateMu.Lock()
	c.orderResp[clientOrdID] = ch
	c.privateMu.Unlock()

	defer func() {
		c.privateMu.Lock()
		delete(c.orderResp, clientOrdID)
		c.privateMu.Unlock()
	}()

	type orderArg struct {
		ClOrdID string `json:"clOrdId"`
		InstID  string `json:"instId"`
		TdMode  string `json:"tdMode"`
		Side    string `json:"side"`
		PosSide string `json:"posSide"`
		OrdType string `json:"ordType"`
		Sz      string `json:"sz"`
	}

	orderMsg := map[string]any{
		"id": reqID,
		"op": "order",
		"args": []orderArg{{
			ClOrdID: clientOrdID,
			InstID:  symbol,
			TdMode:  "cross",
			Side:    side,
			PosSide: posSide,
			OrdType: "market",
			Sz:      sz,
		}},
	}

	if err := conn.WriteJSON(orderMsg); err != nil {
		return "", fmt.Errorf("ws send order: %w", err)
	}

	select {
	case result := <-ch:
		return result.OrderID, result.Error
	case <-time.After(3 * time.Second):
		return "", fmt.Errorf("ws order timeout")
	}
}

func (c *Client) placeOrderREST(ctx context.Context, symbol, side, posSide, sz string) (string, error) {
	body := map[string]any{
		"instId":  symbol,
		"tdMode":  "cross",
		"side":    side,
		"posSide": posSide,
		"ordType": "market",
		"sz":      sz,
	}
	var result []struct {
		OrdID string `json:"ordId"`
		SCode string `json:"sCode"`
		SMsg  string `json:"sMsg"`
	}
	if err := c.privateJSON(ctx, http.MethodPost, "/api/v5/trade/order", nil, body, &result); err != nil {
		return "", err
	}
	if len(result) == 0 {
		return "", fmt.Errorf("no order id returned")
	}
	if result[0].SCode != "0" {
		return "", fmt.Errorf("okx order error: %s %s", result[0].SCode, result[0].SMsg)
	}
	return result[0].OrdID, nil
}

func (c *Client) SetLeverage(ctx context.Context, symbol string, leverage int) error {
	lev := strconv.Itoa(leverage)
	body := map[string]any{"instId": symbol, "lever": lev, "mgnMode": "cross"}
	return c.privateJSON(ctx, http.MethodPost, "/api/v5/account/set-leverage", nil, body, nil)
}

func (c *Client) GetMarkPrice(ctx context.Context, symbol string) (decimal.Decimal, error) {
	var result struct {
		Data []struct {
			MarkPx string `json:"markPx"`
		} `json:"data"`
	}
	resp, err := c.resty.R().SetContext(ctx).
		SetQueryParam("instType", "SWAP").
		SetQueryParam("instId", symbol).
		Get("/api/v5/public/mark-price")
	if err != nil {
		return decimal.Zero, fmt.Errorf("okx mark price: %w", err)
	}
	if err := json.Unmarshal(resp.Body(), &result); err != nil {
		return decimal.Zero, fmt.Errorf("okx parse mark price: %w", err)
	}
	if len(result.Data) == 0 {
		return decimal.Zero, fmt.Errorf("okx mark price not found: %s", symbol)
	}
	price, err := decimal.NewFromString(result.Data[0].MarkPx)
	if err != nil {
		return decimal.Zero, fmt.Errorf("okx parse mark price value: %w", err)
	}
	return price, nil
}

func (c *Client) ClosePosition(ctx context.Context, symbol string, side model.Side) error {
	wantPosSide := "long"
	closeSide := "sell"
	if side == model.SideShort || side == model.SideSell {
		wantPosSide = "short"
		closeSide = "buy"
	}

	// Query the live position: works in both hedge (long/short) and net mode
	var positions []struct {
		PosSide string `json:"posSide"`
		Pos     string `json:"pos"`
	}
	if err := c.privateJSON(ctx, http.MethodGet, "/api/v5/position", map[string]string{"instId": symbol}, nil, &positions); err != nil {
		return fmt.Errorf("okx get position: %w", err)
	}

	posSide := ""
	size := decimal.Zero
	for _, p := range positions {
		pos, err := decimal.NewFromString(p.Pos)
		if err != nil || pos.IsZero() {
			continue
		}
		switch p.PosSide {
		case wantPosSide:
			posSide = p.PosSide
			size = pos.Abs()
		case "net":
			if (wantPosSide == "long" && pos.IsPositive()) || (wantPosSide == "short" && pos.IsNegative()) {
				posSide = "net"
				size = pos.Abs()
			}
		}
		if posSide != "" {
			break
		}
	}
	if posSide == "" || !size.IsPositive() {
		return nil // nothing to close
	}

	body := map[string]any{
		"instId":  symbol,
		"tdMode":  "cross",
		"side":    closeSide,
		"posSide": posSide,
		"ordType": "market",
		"sz":      size.String(),
	}
	var result []struct {
		OrdID string `json:"ordId"`
		SCode string `json:"sCode"`
		SMsg  string `json:"sMsg"`
	}
	if err := c.privateJSON(ctx, http.MethodPost, "/api/v5/trade/order", nil, body, &result); err != nil {
		return fmt.Errorf("okx close order: %w", err)
	}
	if len(result) == 0 {
		return fmt.Errorf("okx close: no order id returned")
	}
	if result[0].SCode != "0" {
		return fmt.Errorf("okx close error: %s %s", result[0].SCode, result[0].SMsg)
	}
	return nil
}

func (c *Client) privateJSON(ctx context.Context, method, path string, query map[string]string, body any, out any) error {
	if c.cfg.APIKey == "" || c.cfg.APISecret == "" {
		return fmt.Errorf("okx api credentials are empty")
	}
	bodyBytes := []byte{}
	if body != nil {
		bodyBytes, _ = sonic.Marshal(body)
	}
	queryString := canonicalQuery(query)
	timestamp := time.UnixMilli(c.nowMs()).UTC().Format("2006-01-02T15:04:05.000Z")
	signPayload := timestamp + method + path
	if queryString != "" {
		signPayload += "?" + queryString
	}
	signPayload += string(bodyBytes)

	mac := hmac.New(sha256.New, []byte(c.cfg.APISecret))
	mac.Write([]byte(signPayload))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	req := c.resty.R().SetContext(ctx).
		SetHeader("OK-ACCESS-KEY", c.cfg.APIKey).
		SetHeader("OK-ACCESS-SIGN", sig).
		SetHeader("OK-ACCESS-TIMESTAMP", timestamp).
		SetHeader("OK-ACCESS-PASSPHRASE", c.cfg.Passphrase).
		SetHeader("Content-Type", "application/json")
	if c.simulated {
		req.SetHeader("x-simulated-trading", "1")
	}
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
		return fmt.Errorf("okx private %s %s: %w", method, path, err)
	}
	if resp.StatusCode() == http.StatusTooManyRequests {
		return fmt.Errorf("okx rate limit")
	}

	return decodeOKXResponse(resp, out)
}

func decodeOKXResponse(resp *resty.Response, out any) error {
	var api struct {
		Code string          `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}
	if err := sonic.Unmarshal(resp.Body(), &api); err != nil {
		return fmt.Errorf("decode okx response status=%d: %w", resp.StatusCode(), err)
	}
	if api.Code != "0" {
		return fmt.Errorf("okx api error code=%s msg=%s data=%s", api.Code, api.Msg, string(api.Data))
	}
	if out != nil && len(api.Data) > 0 {
		if err := sonic.Unmarshal(api.Data, out); err != nil {
			return fmt.Errorf("decode okx data: %w", err)
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
