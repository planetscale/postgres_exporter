FROM --platform=$BUILDPLATFORM pscale.dev/wolfi-prod/go:1.23 AS build
ARG TARGETOS
ARG TARGETARCH
RUN apk --no-cache add curl
COPY . /postgres_exporter
RUN rm -f /postgres_exporter/postgres_exporter
RUN CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" make -C /postgres_exporter build

FROM pscale.dev/wolfi-prod/base:latest
RUN apk --no-cache add curl jq
COPY --from=build /postgres_exporter/postgres_exporter /bin/postgres_exporter
EXPOSE 9104
USER nobody
WORKDIR /
ENTRYPOINT ["/bin/postgres_exporter"]
