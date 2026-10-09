FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o notification-service .

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/notification-service .
EXPOSE 8080
CMD ["./notification-service"]