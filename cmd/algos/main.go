package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"algos/internal/app"
	"algos/internal/config"
	"algos/internal/dex"
	"algos/internal/exchange"
	"algos/internal/exchange/okx"
	"algos/internal/model"
	"algos/internal/monitor"
	"algos/internal/order"
	"algos/internal/risk"
	"algos/internal/storage"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config")
	symbolsFlag := flag.String("symbols", "", "comma-separated symbols to subscribe, e.g. BTCUSDT,ETHUSDT")
	listingFlag := flag.String("listing", "", "optional listing definition SYMBOL:OPEN_PRICE, e.g. NEWUSDT:1.23")
	autoDetect := flag.Bool("auto", false, "auto-detect new listings on OKX")
	dexMonitor := flag.Bool("dex", false, "enable DEX monitoring for pre-listing detection")
	testOrder := flag.String("test-order", "", "place a test market order SYMBOL:SIDE (buy/sell), e.g. BTCUSDT:buy")
	flag.Parse()

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatal("load config", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := storage.OpenBolt(cfg.Storage.BoltPath)
	if err != nil {
		logger.Fatal("open storage", zap.Error(err))
	}
	defer store.Close()

	notifier, err := monitor.NewTelegramNotifier(ctx, cfg.Monitor)
	if err != nil {
		logger.Fatal("create notifier", zap.Error(err))
	}

	riskManager := risk.NewManager(cfg.Bot)
	var client exchange.Client
	var exchangeName string

	// OKX is the only supported exchange
	if okxCfg, ok := cfg.Exchanges["okx"]; ok && okxCfg.Enabled {
		client = okx.New(okxCfg, logger, cfg.Bot)
		exchangeName = "okx"
		logger.Info("using OKX exchange")
	} else {
		logger.Fatal("no exchange enabled: set exchanges.okx.enabled=true")
	}

	orderManager := order.NewManager(cfg, client, riskManager, store, notifier, logger)
	engine := app.NewEngine(cfg, riskManager, orderManager, logger)
	restoreOpenPositions(ctx, store, orderManager, engine, notifier, logger)
	go orderManager.RunMonitor(ctx)

	if *testOrder != "" {
		placeTestOrder(ctx, client, exchangeName, *testOrder, logger)
		return
	}

	symbols := splitCSV(*symbolsFlag)
	if *listingFlag != "" {
		parts := strings.SplitN(*listingFlag, ":", 2)
		if len(parts) == 2 {
			symbols = appendUnique(symbols, parts[0])
			registerListingFlag(engine, exchangeName, *listingFlag, logger)
		} else {
			logger.Warn("invalid -listing, expected SYMBOL:OPEN_PRICE", zap.String("value", *listingFlag))
		}
	}

	if *autoDetect {
		go runListingDetector(ctx, client, exchangeName, engine, logger)
	}

	if *dexMonitor {
		dexMon := dex.NewMonitor(logger, 60*time.Second)
		dexMon.Start(ctx, func(token dex.Token) {
			logger.Info("DEX candidate",
				zap.String("symbol", token.Symbol),
				zap.String("chain", token.Chain),
				zap.Float64("score", token.Score),
				zap.Float64("volume_h1", token.VolumeH1),
				zap.Float64("price_change", token.PriceChange),
			)
		})
		logger.Info("DEX monitor started", zap.Duration("interval", 60*time.Second))
	}

	if len(symbols) == 0 && !*autoDetect {
		logger.Info("no symbols provided; use -symbols=BTCUSDT, -auto, or -listing=SYMBOL:PRICE")
		<-ctx.Done()
		return
	}

	logger.Info("starting public market data", zap.Strings("symbols", symbols), zap.String("mode", cfg.Bot.Mode))
	if err := client.ConnectPublic(ctx, symbols, engine); err != nil && ctx.Err() == nil {
		logger.Fatal("public market data stopped", zap.Error(err))
	}
	orderManager.CloseAll(context.Background())
}

func restoreOpenPositions(ctx context.Context, store *storage.BoltStore, orders *order.Manager, engine *app.Engine, notifier monitor.Notifier, logger *zap.Logger) {
	positions, err := store.LoadOpenPositions()
	if err != nil {
		logger.Error("load open positions", zap.Error(err))
		return
	}
	if len(positions) == 0 {
		return
	}

	logger.Warn("open positions found in storage", zap.Int("count", len(positions)))
	for i, pos := range positions {
		if i > 0 {
			logger.Error("extra open position ignored (single-position bot), close it manually",
				zap.String("id", pos.ID), zap.String("symbol", pos.Symbol))
			if err := notifier.Notify(ctx, fmt.Sprintf("⚠️ extra open position ignored, close manually: %s %s (%s)", pos.Symbol, pos.Side, pos.ID)); err != nil {
				logger.Warn("notify extra position", zap.Error(err))
			}
			continue
		}
		if err := orders.RestorePosition(pos); err != nil {
			logger.Error("restore position", zap.String("id", pos.ID), zap.Error(err))
			continue
		}
		engine.MarkEntered(pos.Exchange, pos.Symbol)
		logger.Warn("restored open position",
			zap.String("id", pos.ID),
			zap.String("symbol", pos.Symbol),
			zap.String("side", string(pos.Side)),
			zap.String("entry", pos.EntryPrice.String()),
			zap.String("qty", pos.Quantity.String()),
		)
		if err := notifier.Notify(ctx, fmt.Sprintf("♻️ restored open position: %s %s | entry %s | qty %s", pos.Symbol, pos.Side, pos.EntryPrice, pos.Quantity)); err != nil {
			logger.Warn("notify restore", zap.Error(err))
		}
	}
}

func runListingDetector(ctx context.Context, client exchange.Client, exchangeName string, engine *app.Engine, logger *zap.Logger) {
	logger.Info("listing detector started, polling every 10s", zap.String("exchange", exchangeName))

	known := make(map[string]bool)

	// Seed with all current symbols on first run
	symbols, err := client.ListSymbols(ctx)
	if err != nil {
		logger.Warn("initial list symbols failed", zap.Error(err))
	} else {
		for _, s := range symbols {
			known[s] = true
		}
		logger.Info("listing detector seeded", zap.Int("known_symbols", len(known)))
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}

		symbols, err := client.ListSymbols(ctx)
		if err != nil {
			logger.Warn("list symbols failed", zap.Error(err))
			continue
		}

		var newSymbols []string
		for _, s := range symbols {
			if !known[s] {
				known[s] = true
				newSymbols = append(newSymbols, s)
			}
		}

		if len(newSymbols) == 0 {
			continue
		}

		logger.Info("NEW LISTING DETECTED", zap.Strings("symbols", newSymbols))

		for _, s := range newSymbols {
			engine.RegisterListing(exchangeName, s, time.Now().UTC(), decimal.Zero)
		}

		if err := client.SubscribeSymbols(newSymbols); err != nil {
			logger.Warn("subscribe new symbols failed", zap.Error(err))
		}
	}
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func appendUnique(slice []string, s string) []string {
	for _, v := range slice {
		if v == s {
			return slice
		}
	}
	return append(slice, s)
}

func placeTestOrder(ctx context.Context, client exchange.Client, exchangeName, value string, logger *zap.Logger) {
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 {
		logger.Fatal("test-order format: SYMBOL:SIDE (buy/sell)", zap.String("value", value))
	}
	symbol := parts[0]
	side := model.Side(strings.ToLower(parts[1]))
	if side != model.SideBuy && side != model.SideSell {
		logger.Fatal("side must be buy or sell", zap.String("side", string(side)))
	}

	logger.Info("placing test order", zap.String("symbol", symbol), zap.String("side", string(side)))

	// Get current price
	price, err := client.GetMarkPrice(ctx, symbol)
	if err != nil {
		logger.Fatal("get mark price", zap.Error(err))
	}
	logger.Info("current price", zap.String("symbol", symbol), zap.String("price", price.String()))

	// Place a minimal market order (lot size = 0.01 for BTC-USDT-SWAP)
	qty := decimal.RequireFromString("0.01")
	pos := &model.Position{
		ID:         fmt.Sprintf("test-%s-%d", symbol, time.Now().UnixNano()),
		Exchange:   exchangeName,
		Symbol:     symbol,
		Side:       side,
		EntryPrice: price,
		Quantity:   qty,
	}

	orderID, err := client.PlaceOrder(ctx, pos)
	if err != nil {
		logger.Fatal("place test order", zap.Error(err))
	}
	logger.Info("TEST ORDER PLACED", zap.String("orderID", orderID), zap.String("symbol", symbol), zap.String("side", string(side)), zap.String("qty", qty.String()))
	logger.Info("check your exchange account — you should see the order there", zap.String("exchange", exchangeName))

	<-ctx.Done()
}

func registerListingFlag(engine *app.Engine, exchangeName, value string, logger *zap.Logger) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		logger.Warn("invalid listing flag, expected SYMBOL:OPEN_PRICE", zap.String("value", value))
		return
	}
	price, err := decimal.NewFromString(parts[1])
	if err != nil {
		logger.Warn("invalid listing price", zap.String("value", value), zap.Error(err))
		return
	}
	engine.RegisterListing(exchangeName, parts[0], time.Now().UTC(), price)
}
