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


.PHONY: deb
deb: release
	rm -rf pkg_root/
	mkdir -p pkg_root/lib/systemd/system/
	cp dist/chronodium.service pkg_root/lib/systemd/system/chronodium.service
	mkdir -p pkg_root/etc/default
	cp dist/debian/defaults pkg_root/etc/default/chronodium
	mkdir -p pkg_root/usr/bin/
	cp bin/chronodium pkg_root/usr/bin/chronodium
	mkdir -p pkg_root/usr/share/doc/chronodium
	cp LICENSE pkg_root/usr/share/doc/chronodium/
	mkdir -p pkg_root/etc/chronodium
	cp chronodium.conf.dist pkg_root/etc/chronodium/chronodium.conf
	mkdir -p pkg_root/etc/logrotate.d
	cp dist/debian/logrotate pkg_root/etc/logrotate.d/chronodium
	fpm \
		-n chronodium \
		-C pkg_root \
		-s dir \
		-t deb \
		-v $(VERSION) \
		--force \
		--deb-compression bzip2 \
		--after-install dist/debian/postinst \
		--before-remove dist/debian/prerm \
		--license Apache-2 \
		-m "Dolf Schimmel <dolf@transip.nl>" \
		--url "https://github.com/Freeaqingme/Chrono-TS" \
		--vendor "github.com/Freeaqingme" \
		--description "Keeping time in Series" \
		--category network \
		--config-files /etc/chronodium/chronodium.conf \
		--directories /var/run/chronodium \
		.

