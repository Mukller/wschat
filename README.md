<div align="center">

[English](README_EN.md) • **Русский**

</div>

# wschat

Асинхронный WebSocket чат-сервер на Rust. Несколько комнат, broadcast,
встроенный веб-клиент. Запускается одной командой.

## Запуск

```bash
cargo build --release
./target/release/wschat

# другой порт
wschat --port 9090

# с логами
RUST_LOG=info wschat
```

## Подключение

- Браузер: `http://localhost:8080` — встроенный клиент
- WebSocket: `ws://localhost:8080/ws/general`
- Своя комната: `ws://localhost:8080/ws/myroom`

## Команды

| Команда | Действие |
|---------|----------|
| `/nick Имя` | Сменить ник |
| `/who` | Список участников |
| `/rooms` | Активные комнаты |

## Что умеет

- Несколько независимых комнат (создаются на лету)
- Broadcast сообщений всем участникам
- Встроенный HTML-клиент
- Нет внешних зависимостей кроме tokio
