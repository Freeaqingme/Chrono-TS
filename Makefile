export GOPATH:=$(shell pwd)

GO        ?= go
PKG       := ./src/Sladu/
BUILDTAGS := debug
VERSION   ?= $(shell git describe --dirty --tags | sed 's/^v//' )

.PHONY: default
default: all

# find src/ -name .git -type d | sed -s 's/.git$//' | while read line; do echo -n "${line} " | sed 's/.\/src\///'; git -C $line rev-parse HEAD; done | sort
.PHONY: deps
deps:
	go get -tags '$(BUILDTAGS)' -d -v Sladu/...
	go get github.com/robfig/glock
	git diff /dev/null GLOCKFILE | ./bin/glock apply .

.PHONY: Sladu
Sladu: deps binary

.PHONY: binary
binary: LDFLAGS += -X "main.buildTag=v$(VERSION)"
binary: LDFLAGS += -X "main.buildTime=$(shell date -u '+%Y-%m-%d %H:%M:%S UTC')"
binary:
	go install -tags '$(BUILDTAGS)' -ldflags '$(LDFLAGS)' Sladu

.PHONY: release
release: BUILDTAGS=release
release: Sladu

.PHONY: fmt
fmt:
	go fmt Sladu/...

.PHONY: all
all: fmt Sladu

.PHONY: clean
clean:
	rm -rf bin/
	rm -rf pkg/
	rm -rf src/Sladu/assets/
	go clean -i -r Sladu

.PHONY: test
test:
	go test -tags '$(BUILDTAGS)' -ldflags '$(LDFLAGS)' Sladu/...
