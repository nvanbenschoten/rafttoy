GO   ?= go
MOD  := -mod=vendor
PKGS := $$($(GO) list ./... | grep -v /vendor/)

.PHONY: build
build:
	$(GO) test -c $(MOD) -o raft-toy-leader
	$(GO) build   $(MOD) -o raft-toy-follower

.PHONY: test
test:
	@$(GO) test $(MOD) -v ./...

.PHONY: bench
bench:
	@$(GO) test $(MOD) -v -run=XXX -bench=. ./...

.PHONY: lint
lint:
	golint $(PKGS)
	$(GO) vet $(PKGS)

.PHONY: vendor
vendor:
	@$(GO) mod vendor

.PHONY: proto
proto:
	@$(MAKE) -C transport/transportpb regenerate
