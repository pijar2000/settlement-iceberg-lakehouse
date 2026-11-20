# MinIO no longer publishes community images to Docker Hub/Quay, so build pinned releases from source.
FROM golang:1.24-bookworm AS build
ARG MINIO_TAG=RELEASE.2025-04-22T22-12-26Z
ARG MC_TAG=RELEASE.2025-04-16T18-13-26Z
ENV CGO_ENABLED=0 GOBIN=/out
RUN go install github.com/minio/minio@${MINIO_TAG} && go install github.com/minio/mc@${MC_TAG}

FROM alpine:3.20
COPY --from=build /out/minio /out/mc /usr/local/bin/
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
