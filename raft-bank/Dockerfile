FROM golang:1.22
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o /bin/server ./cmd/server && go build -o /bin/client ./cmd/client && go build -o /bin/tester ./cmd/tester
