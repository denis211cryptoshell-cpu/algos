# ALGOS — Listing Sniper Bot (OKX)

Снайпер-бот для входа в момент листинга токенов на OKX (USDT-перпетуалы). Решение принимается по данным публичного WebSocket, вход — market-ордер через WebSocket (mainnet) с REST-фолбэком, выход по TP/SL/таймауту на основе кэша цен из того же WS.

## Режимы работы

| Режим | Что делает |
|-------|------------|
| `bot.mode: testnet` | OKX demo (simulated trading): `x-simulated-trading: 1`, `wspap.okx.com` |
| `bot.mode: mainnet` | Реальная торговля: `ws.okx.com`, заголовок simulated не отправляется |
| `bot.trading_mode: paper` | Сигналы и позиции считаются виртуально, ордера на биржу не отправляются |
| `bot.trading_mode: live` | Реальные ордера. Для `mainnet + live` обязателен флаг `-confirm-prod` |

## Архитектура

```
┌──────────────────────────────────────────────────────────────┐
│ DEX Monitor (DexScreener)                                     │
│ Volume spike + Price pump + Buy pressure → score              │
│ Сейчас только логирует кандидатов (в торговлю не подключён)   │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ Listing Detector (OKX REST, опрос каждые 10 с)                │
│ Новые USDT-SWAP → RegisterListing + подписка на WS            │
│ + прогрев инструмента (lotSz/minSz, плечо)                    │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ OKX public WebSocket                                          │
│ tickers (last price, side) + books5 (5 уровней стакана)       │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ Engine                                                        │
│ Снапшоты рынка, дедуп входа, кэш цен для мониторинга позиции  │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ Strategy (momentum)                                           │
│ Buy/Sell notional + Bid/Ask imbalance в окне открытия         │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ Order Manager                                                 │
│ WS market-ордер (REST fallback, идемпотентный clOrdId),       │
│ виртуальный TP/SL + max lifetime, paper-режим, метрики        │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│ Risk Manager                                                  │
│ TP/SL, max lifetime, kill switch после N убытков подряд       │
└──────────────────────────────────────────────────────────────┘
```

## Компоненты

| Модуль | Файл | Назначение |
|--------|------|------------|
| OKX client | `internal/exchange/okx/client.go` | REST + WebSocket, авторизация, синхронизация времени, кэш инструментов, WS-ордера |
| Engine | `internal/app/engine.go` | Координатор, снапшоты, защита от повторного входа (`MarkEntered`) |
| Strategy | `internal/strategy/momentum.go` | Momentum-стратегия для листингов |
| Order manager | `internal/order/manager.go` | Жизненный цикл позиции, paper/live, восстановление, метрики латентности |
| Risk manager | `internal/risk/manager.go` | TP/SL, max lifetime, kill switch |
| DEX monitor | `internal/dex/monitor.go` | DexScreener scoring (только логирование) |
| Storage | `internal/storage/bolt.go` | BoltDB: позиции, сделки, сигналы; загрузка открытых позиций при старте |
| Config | `internal/config/config.go` | YAML-конфигурация с валидацией |
| Telegram | `internal/monitor/telegram.go` | Уведомления |

## Быстрый старт

```bash
make build

# конфиги (не коммитятся):
# config.example.yaml — шаблон
# config.testnet.yaml — demo-ключи, data/testnet/trades.db
# config.prod.yaml    — боевые ключи, data/prod/trades.db
cp config.example.yaml config.testnet.yaml   # заполнить API-ключи demo

make run-testnet
```

Запуск напрямую:

```bash
go run ./cmd/algos -config config.testnet.yaml -auto -dex
go run ./cmd/algos -config config.testnet.yaml -symbols BTC-USDT-SWAP
go run ./cmd/algos -config config.testnet.yaml -listing BTC-USDT-SWAP:63000
go run ./cmd/algos -config config.testnet.yaml -test-order BTC-USDT-SWAP:buy
```

### Флаги

| Флаг | Описание |
|------|----------|
| `-config` | путь к YAML (по умолчанию `config.yaml`) |
| `-symbols` | список символов через запятую |
| `-listing` | ручной листинг `SYMBOL:OPEN_PRICE` |
| `-auto` | авто-детект новых листингов OKX |
| `-dex` | включить DEX-монитор (логирование кандидатов) |
| `-test-order` | проверочный market-ордер `SYMBOL:buy/sell` |
| `-confirm-prod` | обязателен для `mainnet + live` |

## Торговая логика

Вход LONG (все условия в окне `opening_window_seconds`):

- `BuyNotional / SellNotional >= trade_velocity_ratio`
- `BidNotional / AskNotional >= imbalance_ratio`
- ликвидность Ask >= `min_liquidity_notional` (если задано)
- `OpeningPrice <= DEXFairPrice` (если включён `dex_fair_price_enabled`)

Вход SHORT — зеркально, плюс `OpeningPrice >= DEXFairPrice * short_overprice_multiplier`.

Выход (первое событие):

- Take Profit: `+tp_percent`
- Stop Loss: `-sl_percent`
- Max lifetime: `max_position_seconds`
- Ручное закрытие при shutdown

TP/SL виртуальные: позиция проверяется каждые `position_check_ms` по цене из WS-кэша (REST mark price — только если WS-цены ещё нет). Закрытие на бирже — reduce-only market-ордер (корректно для hedge-режима).

Kill switch: после `kill_switch_losses` убытков подряд торговля блокируется до рестарта.

## Восстановление после рестарта

Открытая позиция сохраняется в BoltDB. При старте бот:

1. загружает открытую позицию (`LoadOpenPositions`);
2. восстанавливает её мониторинг (TP/SL/таймаут продолжают работать);
3. помечает рынок как уже отработанный, чтобы не открыть вторую позицию;
4. отправляет уведомление в Telegram.

Если в базе больше одной открытой позиции, восстанавливается только первая — остальные требуют ручного закрытия (бот держит одну позицию).

## Латентность

В логах на каждый ордер пишутся `decision_latency` (сигнал → отправка) и `place_latency` (отправка → подтверждение), а каждые 25 ордеров — p50/p95 по обоим метрикам.

## Производительность и ограничения

- используется `books5` — 5 уровней стакана (не 200);
- горячий путь не ходит в REST: правила инструмента и плечо прогреваются заранее (`PrepareSymbol`);
- одновременно держится одна позиция;
- DEX-монитор пока только логирует кандидатов;
- для минимальной задержки нужен VPS рядом с OKX (Токио/Сингапур/Гонконг).

## Тесты

```bash
make test        # go test ./...
make vet         # go vet ./...
make fmt         # gofmt -w .
```

Покрыты: momentum-стратегия, риск-менеджер, paper-выход по TP через монитор позиции, восстановление позиции из BoltDB.

## Деплой на VPS

```bash
sudo apt update && sudo apt install -y golang-go
git clone <repo> /opt/algos && cd /opt/algos
make build

# конфиги
cp config.example.yaml config.testnet.yaml
cp config.example.yaml config.prod.yaml   # затем выставить mode: mainnet

# systemd
sudo cp deploy/algos-testnet.service /etc/systemd/system/
sudo cp deploy/algos-prod.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now algos-testnet
# боевой бот — только после проверки на demo:
# sudo systemctl enable --now algos-prod
```

## Зависимости

| Пакет | Версия | Назначение |
|-------|--------|------------|
| `github.com/bytedance/sonic` | v1.15.2 | Быстрый JSON |
| `github.com/go-resty/resty/v2` | v2.15.3 | HTTP-клиент |
| `github.com/gorilla/websocket` | v1.5.3 | WebSocket |
| `github.com/shopspring/decimal` | v1.4.0 | Точная арифметика |
| `github.com/spf13/viper` | v1.19.0 | YAML-конфигурация |
| `go.etcd.io/bbolt` | v1.3.11 | Key-value хранилище |
| `go.uber.org/zap` | v1.27.0 | Структурированные логи |
| `github.com/go-telegram/bot` | v1.14.0 | Telegram-уведомления |

## Структура проекта

```
algos/
├── cmd/algos/main.go
├── internal/
│   ├── app/engine.go
│   ├── config/config.go
│   ├── dex/monitor.go
│   ├── exchange/
│   │   ├── exchange.go
│   │   └── okx/client.go
│   ├── model/types.go
│   ├── monitor/telegram.go
│   ├── order/manager.go
│   ├── risk/manager.go
│   ├── storage/bolt.go
│   └── strategy/momentum.go
├── deploy/
│   ├── algos-testnet.service
│   └── algos-prod.service
├── config.example.yaml
├── Makefile
├── go.mod
└── go.sum
```

## Лицензия

MIT License
