# Runtime image for invar. The goreleaser pipeline builds the binary with the
# `slatedb` build tag, links it against libslatedb_uniffi with an $ORIGIN
# rpath, and stages per-arch copies of the shared library under
# .build/goreleaser/. This Dockerfile copies both into the image; the library
# is loaded by the binary from the same directory at runtime, so no
# LD_LIBRARY_PATH is needed.
#
# ubuntu:24.04 (glibc 2.39) matches the glibc the Go binaries are linked
# against on the ubuntu-24.04 release runner and the cross gcc toolchain.
FROM ubuntu:24.04

ARG TARGETPLATFORM
ARG TARGETARCH

ENTRYPOINT ["/usr/bin/invar"]
USER 65532:65532

COPY $TARGETPLATFORM/invar /usr/bin/invar
COPY .build/goreleaser/libslatedb_uniffi-$TARGETARCH.so /usr/bin/libslatedb_uniffi.so
