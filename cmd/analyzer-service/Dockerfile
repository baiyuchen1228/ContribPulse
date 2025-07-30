FROM golang:1.23-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/cmd/analyzer-service

RUN go build -o /analyzer-service

CMD ["/analyzer-service"]
