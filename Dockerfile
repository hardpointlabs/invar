# Runtime image for invar. The release pipeline builds the binaries on Ubuntu
# 24.04 runners (glibc 2.39) and stages them per platform; this Dockerfile
# copies the matching binary onto a minimal rootfs cut from the same Ubuntu
# 24.04 release with Chisel, so the build and runtime libc baselines match.
#
# The rootfs contains only what the binary needs at runtime: libc, libgcc_s,
# CA certificates (rustls-native-certs reads /etc/ssl/certs), and the minimal
# filesystem skeleton (including /tmp, the default --data-path parent).
FROM ubuntu:24.04 AS chisel

ARG TARGETARCH
ARG CHISEL_VERSION=1.5.1
ARG CHISEL_SHA256_AMD64=caa1f84ef144c3311d95ee464c1b72dd237e99055a8c58b791a5591abd42d9a1
ARG CHISEL_SHA256_ARM64=081cd2fb438f735cc79a0e6310ffe43f3a06abb312027a7e4b59afa7a2399769

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) sha256="${CHISEL_SHA256_AMD64}" ;; \
      arm64) sha256="${CHISEL_SHA256_ARM64}" ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -sSfL -o /tmp/chisel.tar.gz \
      "https://github.com/canonical/chisel/releases/download/v${CHISEL_VERSION}/chisel_v${CHISEL_VERSION}_linux_${TARGETARCH}.tar.gz"; \
    echo "${sha256}  /tmp/chisel.tar.gz" | sha256sum -c -; \
    tar -xzf /tmp/chisel.tar.gz -C /usr/local/bin chisel; \
    rm /tmp/chisel.tar.gz

WORKDIR /rootfs

RUN chisel cut --release ubuntu-24.04 \
    --root /rootfs \
    base-files_base \
    base-files_release-info \
    libc6_libs \
    libgcc-s1_libs \
    ca-certificates_data

FROM scratch

COPY --from=chisel /rootfs /

ENTRYPOINT ["/usr/bin/invar"]
USER 65532:65532

ARG TARGETPLATFORM
COPY $TARGETPLATFORM/invar /usr/bin/invar
