# Redis in Go

A Redis-compatible TCP server implemented in Go, supporting core list commands including blocking operations. This is part of the Codecrafters Redis challenge.

## Supported Commands

| Command | Description |
|---------|-------------|
| `RPUSH key value [value ...]` | Append elements to the tail of a list |
| `LPUSH key value [value ...]` | Prepend elements to the head of a list |
| `LPOP key [count]` | Remove and return elements from the head of a list |
| `LRANGE key start stop` | Return a range of elements from a list |
| `BLPOP key timeout` | Blocking LPOP — waits for an element if the list is empty |

## Architecture

### Connection Handling

Each client connection is handled in its own goroutine, spawned from the main accept loop:

```
main() → net.Listen → Accept loop → go HandleConnection(conn, store)
```

The store is created once at server startup and shared across all connections, rather than per-connection. This is required for commands like `BLPOP` to work correctly across clients.

### Store

The `Store` struct holds all server state:

```go
type Store struct {
    mu       sync.Mutex
    lists    map[string]*list.List
    requests map[string]*list.List
}
```

`lists` holds the actual data. `requests` holds pending `BLPOP` registrations per key, as a FIFO queue of `Request` structs.

### RESP Protocol

Commands are parsed from the [RESP (REdis Serialization Protocol)](https://redis.io/docs/reference/protocol-spec/) wire format. Responses are written back as RESP integers, bulk strings, arrays, and null bulk strings.

### BLPOP Implementation

`BLPOP` follows a check-then-register pattern, encapsulated atomically in `LPopOrRegister`:

- If the target list is non-empty, pop and return immediately
- Otherwise, register a `Request{conn, timeout}` in `store.requests[key]`

When `RPUSH` or `LPUSH` adds elements, after pushing all values under the store mutex, `notifyBlpopLocked` is called to serve any waiting clients by popping from the list and writing directly to their connection.

This means the push and notify happen as a single atomic operation — no element can be consumed by a concurrent `BLPOP` registration between the push and the notification.

### Concurrency

A single store-level `sync.Mutex` guards all access to `lists` and `requests`. Key design decisions:

- **Push + notify are atomic**: `PushBack`/`PushFront` hold the lock across the entire operation including `notifyBlpopLocked`, preventing races between concurrent pushers and waiters
- **Register + check are atomic**: `LPopOrRegister` holds the lock across both the list check and the request registration, preventing two concurrent `BLPOP` calls from both registering when one should have popped immediately
- **Unlocked internal methods**: `notifyBlpopLocked` has no lock of its own and is only called from within an already-locked context, following the Go convention of `Locked`-suffixed internal helpers

### Correctness: RPUSH with Multiple Elements

`RPUSH key a b c` is treated as a single atomic operation. All elements are added to the list first, then waiting `BLPOP` clients are notified. This matches Redis semantics — a `BLPOP` waiter receives one element popped from the fully-populated list, not the first element as it arrives.

The length returned to the `RPUSH` caller reflects all pushed elements, including any consumed by `BLPOP` (`l.Len() + notified`).

## Testing

Tests are organized per command in separate `_test.go` files within the same package:

```
app/
  main_test.go     # shared helpers
  push_test.go
  lpop_test.go
  lrange_test.go
  blpop_test.go
```

Each test spins up a real TCP server with an injected store via `setupTestConnectionWithStore`, allowing pre-population of state without going through the wire protocol:

```go
store := StoreWithList("list", []string{"a", "b", "c"})
conn, cleanup := setupTestConnectionWithStore(t, store)
defer cleanup()
```

Race condition tests use multiple connections against the same shared store and are verified with:

```bash
go test -race ./...
```

## Running

```bash
go run ./app
```

Listens on `0.0.0.0:6379` by default. Connect with any Redis client:

```bash
redis-cli RPUSH list a b c
redis-cli BLPOP list 0
```