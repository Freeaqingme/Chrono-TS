export GOPATH:=$(shell pwd)

GO        ?= go
PKG       := ./src/chronodium/
BUILDTAGS := debug
VERSION   ?= $(shell git describe --dirty --tags | sed 's/^v//' )

.PHONY: default
default: all

# find src/ -name .git -type d | sed -s 's/.git$//' | while read line; do echo -n "${line} " | sed 's/.\/src\///'; git -C $line rev-parse HEAD; done | sort
.PHONY: deps
deps:
	go get -tags '$(BUILDTAGS)' -d -v chronodium/...
	go get github.com/robfig/glock
	git diff /dev/null GLOCKFILE | ./bin/glock apply .

.PHONY: chronodium
chronodium: deps binary

.PHONY: binary
binary: LDFLAGS += -X "main.buildTag=v$(VERSION)"
binary: LDFLAGS += -X "main.buildTime=$(shell date -u '+%Y-%m-%d %H:%M:%S UTC')"
binary:
	go install -tags '$(BUILDTAGS)' -ldflags '$(LDFLAGS)' chronodium

.PHONY: release
release: BUILDTAGS=release
release: chronodium

.PHONY: fmt
fmt:
	go fmt chronodium/...

.PHONY: all
all: fmt chronodium

.PHONY: clean
clean:
	rm -rf bin/
	rm -rf pkg/
	rm -rf src/chronodium/assets/
	go clean -i -r chronodium

.PHONY: test
test:
	go test -tags '$(BUILDTAGS)' -ldflags '$(LDFLAGS)' chronodium/...
