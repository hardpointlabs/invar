#!/usr/bin/env bash
# Simulates the goreleaser release routine locally without publishing
# anything: cargo-zigbuild cross-builds, archives, and the multi-arch docker
# images (built and --load'd into the local docker daemon, never pushed to
# GHCR/GitHub).
#
# The routine must run on linux/amd64 to match the GitHub release runner.
# This script therefore runs inside an ubuntu:24.04 container with the repo
# mounted read-write and the host docker daemon mounted so `docker buildx`
# works. On Apple Silicon the container runs under amd64 emulation, so the
# first run is slow; subsequent runs reuse the cached cargo target dir.
#
# Prerequisites: Docker running (OrbStack on macOS: `open -a OrbStack`).
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
export PATH="$HOME/.cargo/bin:/usr/local/bin:$PATH"

apt-get update
apt-get install -y --no-install-recommends \
  git ca-certificates curl jq build-essential docker.io

# docker buildx plugin (docker.io does not bundle it on Ubuntu 24.04)
mkdir -p /usr/libexec/docker/cli-plugins
curl -fsSL -o /usr/libexec/docker/cli-plugins/docker-buildx \
  "$(curl -fsSL https://api.github.com/repos/docker/buildx/releases/latest \
    | jq -r '.assets[] | select(.name | endswith(".linux-amd64")) | .browser_download_url' | head -1)"
chmod +x /usr/libexec/docker/cli-plugins/docker-buildx
docker buildx version

# rust toolchain (mirrors dtolnay/rust-toolchain@stable)
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu

# zig + cargo-zigbuild (mirrors mlugg/setup-zig + cargo install)
ZIG_INDEX="$(curl -fsSL https://ziglang.org/download/index.json)"
ZIG_VERSION="$(jq -r 'to_entries[] | select(.key != "master") | .key' <<<"$ZIG_INDEX" | head -1)"
ZIG_TARBALL="$(jq -r --arg v "$ZIG_VERSION" '.[$v]["x86_64-linux"].tarball' <<<"$ZIG_INDEX")"
ZIG_SHASUM="$(jq -r --arg v "$ZIG_VERSION" '.[$v]["x86_64-linux"].shasum' <<<"$ZIG_INDEX")"
curl -fsSL --retry 5 --retry-all-errors --retry-delay 2 -o /tmp/zig.tar.xz "$ZIG_TARBALL"
echo "$ZIG_SHASUM  /tmp/zig.tar.xz" | sha256sum -c -
mkdir -p /opt/zig
tar -xJ -C /opt/zig --strip-components=1 -f /tmp/zig.tar.xz
rm /tmp/zig.tar.xz
ln -sf /opt/zig/zig /usr/local/bin/zig
zig version
cargo install --locked cargo-zigbuild

# goreleaser (latest, same as the CI action)
curl -fsSL -o /tmp/goreleaser.tar.gz \
  https://github.com/goreleaser/goreleaser/releases/latest/download/goreleaser_Linux_x86_64.tar.gz
tar -C /usr/local/bin -xzf /tmp/goreleaser.tar.gz goreleaser
rm /tmp/goreleaser.tar.gz
goreleaser --version

# simulate the release: builds binaries, archives, and docker images (--load);
# pushes nothing to GHCR or GitHub
goreleaser release --snapshot --clean

# hand the build output back to the host user
chown -R "$HOST_UID:$HOST_GID" /workspace/dist /workspace/target 2>/dev/null || true
EOF

echo
echo "Done. Images are in the local docker daemon (e.g. 'docker images | grep invar')."
