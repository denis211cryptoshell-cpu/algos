APP=algos
BIN=bin/$(APP)

.PHONY: build test vet fmt run-testnet run-prod run-listing clean

build:
	go build -o $(BIN) ./cmd/algos

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

# Demo/testnet bot: OKX simulated trading, separate database (data/testnet)
run-testnet: build
	./$(BIN) -config config.testnet.yaml -auto -dex

# Production bot: requires -confirm-prod on top of a separate config/database
run-prod: build
	./$(BIN) -config config.prod.yaml -confirm-prod -auto -dex

# Manual listing run, e.g. make run-listing SYMBOL=BTC-USDT-SWAP PRICE=63000
run-listing: build
	./$(BIN) -config config.testnet.yaml -listing $(SYMBOL):$(PRICE)

clean:
	rm -rf bin
