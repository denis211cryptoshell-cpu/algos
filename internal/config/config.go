package config

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/spf13/viper"
)

type Config struct {
	Bot       BotConfig                 `mapstructure:"bot"`
	Exchanges map[string]ExchangeConfig `mapstructure:"exchanges"`
	Strategy  StrategyConfig            `mapstructure:"strategy"`
	Monitor   MonitorConfig             `mapstructure:"monitor"`
	Storage   StorageConfig             `mapstructure:"storage"`
}

type BotConfig struct {
	Mode               string          `mapstructure:"mode"`
	TradingMode        string          `mapstructure:"trading_mode"`
	MaxPositionSeconds int             `mapstructure:"max_position_seconds"`
	KillSwitchLosses   int             `mapstructure:"kill_switch_losses"`
	MaxLeverage        int             `mapstructure:"max_leverage"`
	TPPercent          decimal.Decimal `mapstructure:"tp_percent"`
	SLPercent          decimal.Decimal `mapstructure:"sl_percent"`
	PositionCheckMS    int             `mapstructure:"position_check_ms"`
	RecvWindow         string          `mapstructure:"recv_window"`
}

type ExchangeConfig struct {
	Enabled    bool   `mapstructure:"enabled"`
	APIKey     string `mapstructure:"api_key"`
	APISecret  string `mapstructure:"api_secret"`
	Passphrase string `mapstructure:"passphrase"`
	BaseURL    string `mapstructure:"base_url"`
	WSPublic   string `mapstructure:"ws_public"`
	WSPrivate  string `mapstructure:"ws_private"`
}

type StrategyConfig struct {
	ImbalanceRatio           decimal.Decimal `mapstructure:"imbalance_ratio"`
	TradeVelocityRatio       decimal.Decimal `mapstructure:"trade_velocity_ratio"`
	LiquidityRadiusPercent   decimal.Decimal `mapstructure:"liquidity_radius_percent"`
	OrderbookRadiusPercent   decimal.Decimal `mapstructure:"orderbook_radius_percent"`
	MaxPositionPctLiquidity  decimal.Decimal `mapstructure:"max_position_pct_liquidity"`
	OpeningWindowSeconds     int             `mapstructure:"opening_window_seconds"`
	DEXFairPriceEnabled      bool            `mapstructure:"dex_fair_price_enabled"`
	DEXFairPrice             decimal.Decimal `mapstructure:"dex_fair_price"`
	ShortOverpriceMultiplier decimal.Decimal `mapstructure:"short_overprice_multiplier"`
	MinLiquidityNotional     decimal.Decimal `mapstructure:"min_liquidity_notional"`
	MaxSpreadPercent         decimal.Decimal `mapstructure:"max_spread_percent"`
	MinTrades                int             `mapstructure:"min_trades"`
}

type MonitorConfig struct {
	TelegramBotToken string `mapstructure:"telegram_bot_token"`
	TelegramChatID   string `mapstructure:"telegram_chat_id"`
}

type StorageConfig struct {
	BoltPath string `mapstructure:"bolt_path"`
}

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")
	v.SetDefault("bot.mode", "testnet")
	v.SetDefault("bot.trading_mode", "paper")
	v.SetDefault("bot.max_position_seconds", 20)
	v.SetDefault("bot.kill_switch_losses", 3)
	v.SetDefault("bot.max_leverage", 3)
	v.SetDefault("bot.tp_percent", "4.0")
	v.SetDefault("bot.sl_percent", "1.5")
	v.SetDefault("bot.position_check_ms", 50)
	v.SetDefault("bot.recv_window", "5000")
	v.SetDefault("strategy.imbalance_ratio", "1.3")
	v.SetDefault("strategy.trade_velocity_ratio", "1.5")
	v.SetDefault("strategy.liquidity_radius_percent", "1.5")
	v.SetDefault("strategy.orderbook_radius_percent", "2.0")
	v.SetDefault("strategy.max_position_pct_liquidity", "10.0")
	v.SetDefault("strategy.opening_window_seconds", 3)
	v.SetDefault("strategy.dex_fair_price_enabled", false)
	v.SetDefault("strategy.dex_fair_price", "0")
	v.SetDefault("strategy.short_overprice_multiplier", "2.5")
	v.SetDefault("strategy.min_liquidity_notional", "0")
	v.SetDefault("strategy.max_spread_percent", "0")
	v.SetDefault("strategy.min_trades", 0)
	v.SetDefault("storage.bolt_path", "data/trades.db")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg, viper.DecodeHook(
		func(from reflect.Type, to reflect.Type, data interface{}) (interface{}, error) {
			if to != reflect.TypeOf(decimal.Decimal{}) {
				return data, nil
			}
			switch v := data.(type) {
			case float64:
				return decimal.NewFromFloat(v), nil
			case int:
				return decimal.NewFromInt(int64(v)), nil
			case int64:
				return decimal.NewFromInt(v), nil
			case string:
				return decimal.NewFromString(v)
			default:
				return data, nil
			}
		},
	)); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) Validate() error {
	if c.Bot.Mode != "testnet" && c.Bot.Mode != "mainnet" {
		return fmt.Errorf("bot.mode must be testnet or mainnet, got %q", c.Bot.Mode)
	}
	if c.Bot.TradingMode == "" {
		c.Bot.TradingMode = "paper"
	}
	if c.Bot.TradingMode != "paper" && c.Bot.TradingMode != "live" {
		return fmt.Errorf("bot.trading_mode must be paper or live, got %q", c.Bot.TradingMode)
	}
	if c.Bot.MaxPositionSeconds <= 0 {
		return fmt.Errorf("bot.max_position_seconds must be positive")
	}
	if c.Bot.KillSwitchLosses <= 0 {
		return fmt.Errorf("bot.kill_switch_losses must be positive")
	}
	if c.Bot.MaxLeverage <= 0 {
		return fmt.Errorf("bot.max_leverage must be positive")
	}
	if !c.Bot.TPPercent.IsPositive() || !c.Bot.SLPercent.IsPositive() {
		return fmt.Errorf("bot tp/sl percents must be positive")
	}
	if c.Strategy.OpeningWindowSeconds <= 0 {
		return fmt.Errorf("strategy.opening_window_seconds must be positive")
	}
	if c.Strategy.MaxPositionPctLiquidity.IsNegative() || c.Strategy.MaxPositionPctLiquidity.GreaterThan(decimal.NewFromInt(100)) {
		return fmt.Errorf("strategy.max_position_pct_liquidity must be in [0, 100]")
	}
	return nil
}

func (c Config) MaxPositionDuration() time.Duration {
	return time.Duration(c.Bot.MaxPositionSeconds) * time.Second
}

func (c Config) OpeningWindow() time.Duration {
	return time.Duration(c.Strategy.OpeningWindowSeconds) * time.Second
}

func (c Config) PositionCheckInterval() time.Duration {
	if c.Bot.PositionCheckMS <= 0 {
		return 50 * time.Millisecond
	}
	return time.Duration(c.Bot.PositionCheckMS) * time.Millisecond
}
