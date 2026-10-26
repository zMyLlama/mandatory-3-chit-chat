# mandatory-3-chit-chat

A distributed chat service where participants can join, exchange messages, and leave the conversation at any time.
Uses bidirectional streaming and Lamport timestamps.

## Requirements

- Go 1.21+

## Run

Start the server:

```
go run ./server
```

Start each client in its own terminal:

```
go run ./client
```

The server listens on port `50051`.

## Usage

- Type a message and press Enter (max 128 characters).
- Leave with Ctrl-C, or end input with Ctrl-D (macOS/Linux) / Ctrl-Z + Enter (Windows).

## Regenerating gRPC code

Only needed if `grpc/*.proto` changes:

```
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative .\grpc\spec.proto
```

## Logs

Server and clients log to the terminal.