FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/consumer .

FROM scratch
COPY --from=build /out/consumer /consumer
USER 65532:65532
ENTRYPOINT ["/consumer"]
