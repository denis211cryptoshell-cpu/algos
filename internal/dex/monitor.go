package dex

import (
	"context"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/bytedance/sonic"
	"go.uber.org/zap"
)

type Monitor struct {
	logger   *zap.Logger
	client   *http.Client
	interval time.Duration
	stopCh   chan struct{}
}

type Token struct {
	Symbol      string
	Name        string
	Chain       string
	Address     string
	PriceUSD    float64
	VolumeH1    float64
	VolumeH24   float64
	PriceChange float64
	BuysH1      int
	SellsH1     int
	Liquidity   float64
	Score       float64
}

type dexScreenerResponse struct {
	Pairs []struct {
		BaseToken struct {
			Address string `json:"address"`
			Name    string `json:"name"`
			Symbol  string `json:"symbol"`
		} `json:"baseToken"`
		ChainID  string  `json:"chainId"`
		PriceUsd float64 `json:"priceUsd"`
		Volume   struct {
			H1  float64 `json:"h1"`
			H24 float64 `json:"h24"`
		} `json:"volume"`
		PriceChange struct {
			H1 float64 `json:"h1"`
		} `json:"priceChange"`
		Txns struct {
			H1 struct {
				Buys  int `json:"buys"`
				Sells int `json:"sells"`
			} `json:"h1"`
		} `json:"txns"`
		Liquidity struct {
			Usd float64 `json:"usd"`
		} `json:"liquidity"`
	} `json:"pairs"`
}

type dexBoostResponse struct {
	ChainID      string `json:"chainId"`
	TokenAddress string `json:"tokenAddress"`
	Description  string `json:"description"`
	TotalAmount  int    `json:"totalAmount"`
}

type onTokenCallback func(token Token)

func NewMonitor(logger *zap.Logger, interval time.Duration) *Monitor {
	return &Monitor{
		logger:   logger.Named("dex-monitor"),
		client:   &http.Client{Timeout: 10 * time.Second},
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

func (m *Monitor) Start(ctx context.Context, callback onTokenCallback) {
	go func() {
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()

		// Initial scan
		m.scan(ctx, callback)

		for {
			select {
			case <-ctx.Done():
				return
			case <-m.stopCh:
				return
			case <-ticker.C:
				m.scan(ctx, callback)
			}
		}
	}()
}

func (m *Monitor) Stop() {
	close(m.stopCh)
}

func (m *Monitor) scan(ctx context.Context, callback onTokenCallback) {
	// Get top boosted tokens
	boosted, err := m.getTopBoosted(ctx)
	if err != nil {
		m.logger.Warn("failed to get boosted tokens", zap.Error(err))
		return
	}

	// Get trending tokens
	trending, err := m.getTrending(ctx)
	if err != nil {
		m.logger.Warn("failed to get trending tokens", zap.Error(err))
		return
	}

	// Combine and score
	allTokens := make(map[string]Token)
	for _, t := range boosted {
		allTokens[t.Symbol+t.Chain] = t
	}
	for _, t := range trending {
		if existing, ok := allTokens[t.Symbol+t.Chain]; ok {
			// Merge scores
			existing.Score += t.Score
			allTokens[t.Symbol+t.Chain] = existing
		} else {
			allTokens[t.Symbol+t.Chain] = t
		}
	}

	// Filter and sort by score
	var candidates []Token
	for _, t := range allTokens {
		if t.Score > 0.5 {
			candidates = append(candidates, t)
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	// Report top candidates
	if len(candidates) > 0 {
		m.logger.Info("DEX candidates found",
			zap.Int("count", len(candidates)),
			zap.String("top", candidates[0].Symbol),
			zap.Float64("score", candidates[0].Score),
		)
		for _, t := range candidates {
			if callback != nil {
				callback(t)
			}
		}
	}
}

func (m *Monitor) getTopBoosted(ctx context.Context) ([]Token, error) {
	resp, err := m.client.Get("https://api.dexscreener.com/token-boosts/top/v1")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var boosts []dexBoostResponse
	if err := sonic.Unmarshal(body, &boosts); err != nil {
		return nil, err
	}

	var tokens []Token
	for _, b := range boosts {
		score := float64(b.TotalAmount) / 500.0 // Normalize to 0-1
		if score > 1 {
			score = 1
		}
		tokens = append(tokens, Token{
			Symbol:  extractSymbol(b.Description),
			Chain:   b.ChainID,
			Address: b.TokenAddress,
			Score:   score,
			Name:    b.Description,
		})
	}

	return tokens, nil
}

func (m *Monitor) getTrending(ctx context.Context) ([]Token, error) {
	resp, err := m.client.Get("https://api.dexscreener.com/latest/dex/tokens/trending")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data dexScreenerResponse
	if err := sonic.Unmarshal(body, &data); err != nil {
		return nil, err
	}

	var tokens []Token
	for _, p := range data.Pairs {
		score := m.calculateScore(p)
		if score > 0 {
			tokens = append(tokens, Token{
				Symbol:      p.BaseToken.Symbol,
				Name:        p.BaseToken.Name,
				Chain:       p.ChainID,
				Address:     p.BaseToken.Address,
				PriceUSD:    p.PriceUsd,
				VolumeH1:    p.Volume.H1,
				VolumeH24:   p.Volume.H24,
				PriceChange: p.PriceChange.H1,
				BuysH1:      p.Txns.H1.Buys,
				SellsH1:     p.Txns.H1.Sells,
				Liquidity:   p.Liquidity.Usd,
				Score:       score,
			})
		}
	}

	return tokens, nil
}

func (m *Monitor) calculateScore(pair struct {
	BaseToken struct {
		Address string `json:"address"`
		Name    string `json:"name"`
		Symbol  string `json:"symbol"`
	} `json:"baseToken"`
	ChainID  string  `json:"chainId"`
	PriceUsd float64 `json:"priceUsd"`
	Volume   struct {
		H1  float64 `json:"h1"`
		H24 float64 `json:"h24"`
	} `json:"volume"`
	PriceChange struct {
		H1 float64 `json:"h1"`
	} `json:"priceChange"`
	Txns struct {
		H1 struct {
			Buys  int `json:"buys"`
			Sells int `json:"sells"`
		} `json:"h1"`
	} `json:"txns"`
	Liquidity struct {
		Usd float64 `json:"usd"`
	} `json:"liquidity"`
}) float64 {
	score := 0.0

	// Volume spike (h1 vs h24)
	if pair.Volume.H24 > 0 {
		volumeRatio := pair.Volume.H1 / (pair.Volume.H24 / 24)
		if volumeRatio > 2 {
			score += 0.3
		}
		if volumeRatio > 5 {
			score += 0.2
		}
	}

	// Price change
	if pair.PriceChange.H1 > 50 {
		score += 0.3
	} else if pair.PriceChange.H1 > 20 {
		score += 0.15
	}

	// Buy pressure
	if pair.Txns.H1.Sells > 0 {
		buyRatio := float64(pair.Txns.H1.Buys) / float64(pair.Txns.H1.Sells)
		if buyRatio > 1.5 {
			score += 0.2
		}
	}

	// Liquidity
	if pair.Liquidity.Usd > 10000 {
		score += 0.1
	}
	if pair.Liquidity.Usd > 50000 {
		score += 0.1
	}

	return score
}

func extractSymbol(description string) string {
	if len(description) > 20 {
		return description[:20]
	}
	return description
}
