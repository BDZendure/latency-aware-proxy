GOCACHE ?= /tmp/latencyproxy-go-cache
export GOCACHE
RUN_ID ?= $(shell date -u +%Y%m%dT%H%M%SZ)

.PHONY: build test race check smoke bench clean
build:
	mkdir -p bin
	go build -trimpath -o bin/proxy ./cmd/proxy
	go build -trimpath -o bin/backend ./cmd/backend
	go build -trimpath -o bin/loadgen ./cmd/loadgen
test:
	go test ./...
	python3 -m unittest discover -s bench -p 'test_*.py'
race:
	go test -race ./...
check:
	test -z "$$(gofmt -l cmd internal)"
	go vet ./...
	$(MAKE) test race
smoke: build
	python3 bench/runner.py --scenarios steady --duration 2 --warmup 0.5 --repetitions 1 --output bench/runs/smoke-$(RUN_ID)
bench: build
	python3 bench/runner.py --output bench/runs/full-$(RUN_ID)
clean:
	rm -f bin/proxy bin/backend bin/loadgen
