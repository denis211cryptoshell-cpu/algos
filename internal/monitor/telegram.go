package monitor

import (
	"context"
	"fmt"

	"algos/internal/config"

	telegram "github.com/go-telegram/bot"
)

type Notifier interface {
	Notify(ctx context.Context, text string) error
}

type NoopNotifier struct{}

func (NoopNotifier) Notify(context.Context, string) error { return nil }

type TelegramNotifier struct {
	client *telegram.Bot
	chatID string
}

func NewTelegramNotifier(ctx context.Context, cfg config.MonitorConfig) (Notifier, error) {
	if cfg.TelegramBotToken == "" || cfg.TelegramChatID == "" {
		return NoopNotifier{}, nil
	}
	client, err := telegram.New(cfg.TelegramBotToken)
	if err != nil {
		return nil, fmt.Errorf("create telegram bot: %w", err)
	}
	return &TelegramNotifier{client: client, chatID: cfg.TelegramChatID}, nil
}

func (n *TelegramNotifier) Notify(ctx context.Context, text string) error {
	_, err := n.client.SendMessage(ctx, &telegram.SendMessageParams{ChatID: n.chatID, Text: text})
	return err
}
