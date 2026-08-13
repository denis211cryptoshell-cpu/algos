# ALGOS — Listing Sniper Bot

Высокочастотный торговый бот для автоматического входа при листинге новых токенов на CEX биржах (OKX, Bybit).

## Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                    DEX Monitor (DexScreener)                │
│  Мониторит DEX на предмет аномальной активности            │
│  Volume spike + Price pump + Buy pressure → Score           │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   Listing Detector                          │
│  Каждые 10 сек проверяет OKX на новые токены               │
│  При обнаружении → автоматическая подписка на WS           │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                  OKX WebSocket (public)                     │
│  Tickers + Orderbook (200 уровней) в реальном времени      │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   Strategy Engine                           │
│  Momentum: Buy/Sell ratio + Bid/Ask imbalance              │
│  Opening window: 3 секунды после листинга                  │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   Order Manager                             │
│  OKX REST API → Market orders (cross margin)               │
│  Auto TP/SL + Max hold 20 сек + Kill switch                │
└────────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   Risk Manager                              │
│  TP +4% / SL -1.5% / Max lifetime 20s / Kill after 3 loss │
└─────────────────────────────────────────────────────────────┘
```

## Компоненты

| Модуль | Файл | Назначение |
|--------|------|------------|
| **OKX Client** | `internal/exchange/okx/client.go` | REST API + WebSocket, авторизация, time sync |
| **Bybit Client** | `internal/exchange/bybit/client.go` | REST API + WebSocket (заблокирован регуляторно) |
| **DEX Monitor** | `internal/dex/monitor.go` | Мониторинг DexScreener API, scoring токенов |
| **Strategy** | `internal/strategy/momentum.go` | Momentum стратегия для листингов |
| **Order Manager** | `internal/order/manager.go` | Жизненный цикл позиций, paper/live mode |
| **Risk Manager** | `internal/risk/manager.go` | TP/SL, kill switch, контроль убытков |
| **Engine** | `internal/app/engine.go` | Центральный координатор, обработка событий |
| **Storage** | `internal/storage/bolt.go` | BoltDB для хранения сделок и позиций |
| **Config** | `internal/config/config.go` | YAML конфигурация с валидацией |

## Установка

```bash
git clone <repo>
cd algos
go mod download
go build ./...
```

## Запуск

### Полный режим (DEX монитор + авто-детект OKX)

```bash
go run ./cmd/algos -config config.yaml -auto -dex
```

Бот запускается в режиме ожидания:
- DEX монитор проверяет DexScreener каждые 60 сек
- Listing detector проверяет OKX каждые 10 сек
- При обнаружении нового токена — автоматически подключается и торгует

### С конкретным символом

```bash
go run ./cmd/algos -config config.yaml -symbols BTC-USDT-SWAP
```

### С ручным листингом

```bash
go run ./cmd/algos -config config.yaml -listing BTC-USDT-SWAP:63000
```

### Тест ордера

```bash
go run ./cmd/algos -config config.yaml -test-order BTC-USDT-SWAP:buy
```

## Конфигурация (config.yaml)

```yaml
bot:
  mode: "testnet"              # testnet / mainnet
  trading_mode: "live"         # paper / live
  max_position_seconds: 20     # Максимальное удержание позиции
  kill_switch_losses: 3        # Kill switch после N убытков
  max_leverage: 3              # Максимальное кредитное плечо
  tp_percent: 4.0              # Take profit %
  sl_percent: 1.5              # Stop loss %
  position_check_ms: 50        # Интервал проверки позиции (мс)
  recv_window: "5000"          # OKX recv window (мс)

exchanges:
  okx:
    enabled: true
    api_key: "YOUR_KEY"
    api_secret: "YOUR_SECRET"
    passphrase: "YOUR_PASSPHRASE"
    base_url: "https://www.okx.com"
    ws_public: "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"

strategy:
  imbalance_ratio: 1.3         # Минимальный Bid/Ask ratio
  trade_velocity_ratio: 1.5    # Минимальный Buy/Sell ratio
  opening_window_seconds: 3    # Окно входа после листинга
  max_position_pct_liquidity: 10.0  # Размер позиции от ликвидности
```

## Торговая логика

### Вход (LONG)
Все условия должны быть выполнены в opening window (3 сек):
- `BuyNotional / SellNotional >= 1.5` (покупатели доминируют)
- `BidNotional / AskNotional >= 1.3` ( Bid стена толще Ask)
- `OpeningPrice <= DEXFairPrice` (если DEX фильтр включён)

### Вход (SHORT)
- `SellNotional / BuyNotional >= 1.5` (продавцы доминируют)
- `AskNotional / BidNotional >= 1.3` (Ask стена толще Bid)
- `OpeningPrice >= DEXFairPrice * 2.5` (перекупленность)

### Выход
Первое срабатывание:
- **Take Profit:** +4% от входа
- **Stop Loss:** -1.5% от входа
- **Max Lifetime:** 20 секунд
- **Manual:** при shutdown

### Kill Switch
После 3 убытков подряд — полная блокировка торговли до ручного сброса.

## DEX Monitor

Мониторит DexScreener API для раннего обнаружения токенов перед листингом на CEX.

### Сигналы
| Сигнал | Формула | Порог |
|--------|---------|-------|
| Volume spike | h1_volume / (h24_volume / 24) | > 2x |
| Price pump | h1_price_change | > 50% |
| Buy pressure | buys / sells | > 1.5 |
| Liquidity | liquidity_usd | > $10k |

### Scoring
- Volume spike x2 → +0.3
- Volume spike x5 → +0.2
- Price pump > 50% → +0.3
- Buy ratio > 1.5 → +0.2
- Liquidity > $10k → +0.1
- Liquidity > $50k → +0.1

Score > 0.5 → кандидат на листинг.

## Производительность

| Метрика | Значение |
|---------|----------|
| REST API ping | ~317ms |
| Opening window | 3 сек |
| Запас времени | ~2.7 сек |
| DEX Monitor CPU | +0.05% |
| DEX Monitor память | +150 KB |
| DEX Monitor сеть | 70 KB/мин |

## Деплой на VPS

### Требования
- Ubuntu 22.04+
- Go 1.22+
- VPS рядом с OKX (Токио/Сингапур/Гонконг)

### Установка
```bash
# На VPS
sudo apt update && sudo apt install -y golang-go
git clone <repo>
cd algos
go build -o algos ./cmd/algos

# Запуск
nohup ./algos -config config.yaml -auto -dex > bot.log 2>&1 &
```

### Мониторинг
```bash
# Просмотр логов
tail -f bot.log

# Проверка процесса
ps aux | grep algos

# Остановка
pkill algos
```

## Тестирование

### Запуск тестов
```bash
go test ./... -v
```

### Тест на testnet
1. Создайте API ключи на testnet.okx.com
2. Вставьте в config.yaml
3. Запустите: `go run ./cmd/algos -config config.yaml -auto -dex`

### Проверка баланса
```bash
go run ./cmd/algos -config config.yaml -test-order BTC-USDT-SWAP:buy
```

## Структура проекта

```
algos/
├── cmd/
│   └── algos/
│       └── main.go              # Точка входа
├── internal/
│   ├── app/
│   │   └── engine.go            # Центральный координатор
│   ├── config/
│   │   └── config.go            # Конфигурация
│   ├── dex/
│   │   └── monitor.go           # DEX монитор (DexScreener)
│   ├── exchange/
│   │   ├── exchange.go          # Интерфейс биржи
│   │   ├── bybit/
│   │   │   └── client.go        # Bybit клиент
│   │   └── okx/
│   │       └── client.go        # OKX клиент
│   ├── model/
│   │   └── types.go             # Доменные типы
│   ├── monitor/
│   │   └── telegram.go          # Telegram уведомления
│   ├── order/
│   │   └── manager.go           # Управление ордерами
│   ├── risk/
│   │   └── manager.go           # Риск-менеджмент
│   └── storage/
│       └── bolt.go              # BoltDB хранилище
├── config.yaml                  # Конфигурация
├── go.mod                       # Go модули
├── go.sum                       # Зависимости
└── Makefile                     # Команды сборки
```

## Зависимости

| Пакет | Версия | Назначение |
|-------|--------|------------|
| `bytedance/sonic` | v1.15.2 | Быстрый JSON |
| `shopspring/decimal` | v1.4.0 | Точная арифметика |
| `gorilla/websocket` | v1.5.3 | WebSocket клиент |
| `go-resty/resty` | v2.15.3 | HTTP клиент |
| `go-telegram/bot` | v1.14.0 | Telegram бот |
| `spf13/viper` | v1.19.0 | YAML конфигурация |
| `go.etcd.io/bbolt` | v1.3.11 | Key-value хранилище |
| `go.uber.org/zap` | v1.27.0 | Структурированные логи |

## Лицензия

MIT License
