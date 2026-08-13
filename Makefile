APP=algos
BIN=bin/$(APP)

.PHONY: build test run clean

build:
	go build -o $(BIN) ./cmd/algos

test:
	go test ./...

run:
	go run ./cmd/algos -config config.yaml

clean:
	rm -rf bin data