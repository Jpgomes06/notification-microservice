FROM golang:1.27-alpine AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/producer .

FROM scratch
COPY --from=build /out/producer /producer
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/producer"]
