GO   ?= go

.PHONY: build
build:
	$(GO) test -c -o rafttoy-leader
	$(GO) build   -o rafttoy-follower

.PHONY: test
test:
	@$(GO) test -v ./...

.PHONY: bench
bench:
	@$(GO) test -v -run=XXX -bench=. ./...

.PHONY: lint
lint:
	golint ./...
	$(GO) vet ./...

.PHONY: proto
proto:
	@$(MAKE) -C config regenerate
	@$(MAKE) -C transport/transportpb regenerate
