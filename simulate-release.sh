#!/usr/bin/env bash
# Simulates the goreleaser release routine locally without publishing
# anything: Go builds, archives, and the multi-arch docker images (built and
# --load'd into the local docker daemon, never pushed to GHCR/GitHub).
#
# The routine must run on linux/amd64 to match the GitHub release runner
# (goreleaser builds the amd64 binary with the native `gcc`). This script
# therefore runs inside an ubuntu:24.04 container with the repo mounted
# read-write and the host docker daemon mounted so `docker buildx` works. On
# Apple Silicon the container runs under amd64 emulation, so the first run is
# slow while the SlateDB Rust libs compile; subsequent runs reuse the cached
# .build/slatedb checkout and cargo target dir.
#
# Prerequisites: Docker running (OrbStack on macOS: `open -a OrbStack`).
#
# Afterwards: `make clean` restores the injected go.mod and removes .build/.
set -euo pipefail

IMAGE=ubuntu:24.04
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! docker info >/dev/null 2>&1; then
  echo "error: Docker is not running. On macOS with OrbStack, start it with: open -a OrbStack" >&2
  exit 1
fi

docker run --rm -i \
  --platform linux/amd64 \
  -v "$ROOT:/workspace" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e HOST_UID="$(id -u)" \
  -e HOST_GID="$(id -g)" \
  -w /workspace \
  "$IMAGE" bash -s <<'EOF'
set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive
export PATH="/usr/local/go/bin:$HOME/.cargo/bin:/usr/local/bin:$PATH"

apt-get update
apt-get install -y --no-install-recommends \
  git ca-certificates curl jq build-essential cmake perl pkg-config \
  docker.io gcc-aarch64-linux-gnu libc6-dev-arm64-cross

# docker buildx plugin (docker.io does not bundle it on Ubuntu 24.04)
mkdir -p /usr/libexec/docker/cli-plugins
curl -fsSL -o /usr/libexec/docker/cli-plugins/docker-buildx \
  "$(curl -fsSL https://api.github.com/repos/docker/buildx/releases/latest \
    | jq -r '.assets[] | select(.name | endswith(".linux-amd64")) | .browser_download_url' | head -1)"
chmod +x /usr/libexec/docker/cli-plugins/docker-buildx
docker buildx version

# go (mirrors actions/setup-go 'stable')
GO_VERSION="$(curl -fsSL 'https://go.dev/dl/?mode=json' | jq -r '.[0].version' | sed 's/^go//')"
curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" | tar -C /usr/local -xz
go version

# rust toolchain (SlateDB pins its own via rust-toolchain.toml)
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal

# goreleaser (latest, same as the CI action)
curl -fsSL -o /tmp/goreleaser.tar.gz \
  https://github.com/goreleaser/goreleaser/releases/latest/download/goreleaser_Linux_x86_64.tar.gz
tar -C /usr/local/bin -xzf /tmp/goreleaser.tar.gz goreleaser
rm /tmp/goreleaser.tar.gz
goreleaser --version

# SlateDB libs: amd64 native + arm64 via plain cargo cross-compile, staged
# for goreleaser
make stage-goreleaser-assets

# simulate the release: builds binaries, archives, and docker images (--load);
# pushes nothing to GHCR or GitHub
goreleaser release --snapshot --clean

# hand the build output back to the host user
chown -R "$HOST_UID:$HOST_GID" /workspace/.build /workspace/dist 2>/dev/null || true
EOF

echo
echo "Done. Images are in the local docker daemon (e.g. 'docker images | grep invar')."
echo "Restore the repo state with: make clean"
