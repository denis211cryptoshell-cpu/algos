package storage

import (
	"github.com/bytedance/sonic"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"algos/internal/model"

	bbolt "go.etcd.io/bbolt"
)

var (
	positionsBucket = []byte("positions")
	eventsBucket    = []byte("events")
	tradesBucket    = []byte("trades")
	signalsBucket   = []byte("signals")
	errorsBucket    = []byte("errors")
)

type StoredTrade struct {
	ID         string    `json:"id"`
	Symbol     string    `json:"symbol"`
	Side       string    `json:"side"`
	EntryPrice string    `json:"entry_price"`
	ExitPrice  string    `json:"exit_price"`
	Quantity   string    `json:"quantity"`
	PnL        string    `json:"pnl"`
	EntryTime  time.Time `json:"entry_time"`
	ExitTime   time.Time `json:"exit_time"`
	Duration   int64     `json:"duration"`
	Reason     string    `json:"reason"`
}

type BoltStore struct {
	db *bbolt.DB
}

func OpenBolt(path string) (*BoltStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create bolt dir: %w", err)
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bolt: %w", err)
	}
	store := &BoltStore{db: db}
	if err := store.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *BoltStore) init() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(positionsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(eventsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(tradesBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(signalsBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(errorsBucket)
		return err
	})
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}

func (s *BoltStore) SavePosition(position model.Position) error {
	payload, err := sonic.Marshal(position)
	if err != nil {
		return fmt.Errorf("marshal position: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(positionsBucket).Put([]byte(position.ID), payload)
	})
}

func (s *BoltStore) SaveEvent(key string, value any) error {
	payload, err := sonic.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(eventsBucket).Put([]byte(key), payload)
	})
}

func (s *BoltStore) SaveTrade(trade StoredTrade) error {
	payload, err := sonic.Marshal(trade)
	if err != nil {
		return fmt.Errorf("marshal stored trade: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(tradesBucket).Put([]byte(trade.ID), payload)
	})
}

func (s *BoltStore) SaveSignal(signal model.Signal) error {
	payload, err := sonic.Marshal(signal)
	if err != nil {
		return fmt.Errorf("marshal signal: %w", err)
	}
	key := fmt.Sprintf("%s:%s:%d", signal.Exchange, signal.Symbol, signal.CreatedAt.UnixNano())
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(signalsBucket).Put([]byte(key), payload)
	})
}

func (s *BoltStore) SaveError(key string, value any) error {
	payload, err := sonic.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(errorsBucket).Put([]byte(key), payload)
	})
}
