<div align="center">

[Русский](README.md) • **English**

</div>

# wschat

Async WebSocket chat server in Rust. Multiple rooms, broadcast messages,
built-in web client. Runs with one command.

## Run

```bash
cargo build --release
./target/release/wschat

# custom port
wschat --port 9090

# with logs
RUST_LOG=info wschat
```

## Connect

- Browser: `http://localhost:8080` — built-in client
- WebSocket: `ws://localhost:8080/ws/general`
- Custom room: `ws://localhost:8080/ws/myroom`

## Commands

| Command | Action |
|---------|--------|
| `/nick Name` | Change nickname |
| `/who` | List room members |
| `/rooms` | Active rooms |

## Features

- Multiple independent rooms (created on the fly)
- Broadcast to all room members
- Built-in HTML client
- No external dependencies beyond tokio
