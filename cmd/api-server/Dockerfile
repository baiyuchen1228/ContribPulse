FROM golang:1.22-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

WORKDIR /app/cmd/api-server

RUN go build -o /api-server

EXPOSE 8080

CMD ["/api-server"]
