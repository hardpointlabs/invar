# Runtime image for invar. The goreleaser pipeline builds the release
# binaries with cargo-zigbuild against glibc 2.31 and stages them per
# platform; this Dockerfile copies the matching binary into a minimal base.
#
# distroless/cc-debian12 (glibc 2.36) ships everything the binary needs at
# runtime: libc, libgcc_s, and CA certificates.
FROM gcr.io/distroless/cc-debian12

ARG TARGETPLATFORM

ENTRYPOINT ["/usr/bin/invar"]
USER 65532:65532

COPY $TARGETPLATFORM/invar /usr/bin/invar
