# wschat

Async WebSocket chat server in Rust. Multiple rooms, broadcast, built-in web client.

## Usage

```bash
cargo install wschat
wschat              # port 8080
wschat --port 9090
```

## Commands

| Command | Description |
|---------|-------------|
| /nick Name | Change nickname |
| /who | List room participants |
| /rooms | List active rooms |
