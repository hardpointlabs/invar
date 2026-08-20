# Build harness for the optional SlateDB backend.
#
# The SlateDB Go bindings (slatedb.io/slatedb-go) require the native
# libslatedb_uniffi shared library plus the corresponding module wiring in
# go.mod. Both are produced by `make`. The committed go.mod intentionally has
# no SlateDB references, so a plain `go build`/`go test` works with only the
# BadgerDB backend. kv/slate.go and kv/slate_test.go are gated behind the
# `slatedb` build tag and are only compiled when GO_TAGS includes it.
#
# Release builds no longer use this Makefile: goreleaser builds the Rust
# implementation directly (see .goreleaser.yaml).

SLATEDB_DIR           := .build/slatedb
SLATEDB_REF           := bindings/go/v0.15.0
SLATEDB_LIB_DIR       := $(abspath $(SLATEDB_DIR))/target/release
GO_TAGS               ?= slatedb
UNIFFI_BINDGEN_GO_TAG := v0.7.0+v0.31.0

CGO_ENV      := CGO_ENABLED=1 CGO_LDFLAGS="-L$(SLATEDB_LIB_DIR) -Wl,-rpath,$(SLATEDB_LIB_DIR)"
LIB_PATH_ENV := LD_LIBRARY_PATH="$(SLATEDB_LIB_DIR):$$LD_LIBRARY_PATH" \
                DYLD_LIBRARY_PATH="$(SLATEDB_LIB_DIR):$$DYLD_LIBRARY_PATH"

.PHONY: all deps-slatedb build test test-badger regen-bindings clean

all: deps-slatedb build

$(SLATEDB_DIR)/.git:
	git clone https://github.com/slatedb/slatedb $(SLATEDB_DIR)

deps-slatedb: $(SLATEDB_DIR)/.git
	git -C $(SLATEDB_DIR) checkout --quiet $(SLATEDB_REF)
	cargo build --manifest-path $(SLATEDB_DIR)/Cargo.toml -p slatedb-uniffi --release
	go mod edit -require=slatedb.io/slatedb-go@v0.0.0 \
		-replace=slatedb.io/slatedb-go=./$(SLATEDB_DIR)/bindings/go

build: deps-slatedb
	$(CGO_ENV) go build -tags $(GO_TAGS) -o invar .

test: deps-slatedb
	$(CGO_ENV) $(LIB_PATH_ENV) go test -tags $(GO_TAGS) ./...

test-badger:
	go test ./...

regen-bindings: $(SLATEDB_DIR)/.git
	cargo install uniffi-bindgen-go --git https://github.com/NordSecurity/uniffi-bindgen-go --tag $(UNIFFI_BINDGEN_GO_TAG)
	cd $(SLATEDB_DIR) && ./scripts/generate-go-uniffi.sh

clean:
	rm -rf .build
	go mod edit -droprequire=slatedb.io/slatedb-go 2>/dev/null || true
	go mod edit -dropreplace=slatedb.io/slatedb-go 2>/dev/null || true
