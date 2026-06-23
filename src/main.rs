use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Path, State, WebSocketUpgrade,
    },
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};
use clap::Parser;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{info, warn};
use uuid::Uuid;

// ─── конфиг ───────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "wschat", about = "WebSocket чат-сервер")]
struct Cli {
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Размер буфера broadcast (сообщений в истории)
    #[arg(long, default_value_t = 128)]
    buffer: usize,
}

// ─── состояние ────────────────────────────────────────────────────────────────

#[derive(Clone, Serialize, Deserialize)]
struct ChatMessage {
    room: String,
    author: String,
    text: String,
}

struct Room {
    tx: broadcast::Sender<ChatMessage>,
}

struct AppState {
    rooms: DashMap<String, Arc<Room>>,
    buf_size: usize,
}

impl AppState {
    fn new(buf_size: usize) -> Self {
        Self {
            rooms: DashMap::new(),
            buf_size,
        }
    }

    fn get_or_create_room(&self, name: &str) -> Arc<Room> {
        self.rooms
            .entry(name.to_owned())
            .or_insert_with(|| {
                info!("Создана комната '{name}'");
                let (tx, _) = broadcast::channel(self.buf_size);
                Arc::new(Room { tx })
            })
            .clone()
    }
}

// ─── обработчики ─────────────────────────────────────────────────────────────

async fn ws_handler(
    ws: WebSocketUpgrade,
    Path(room_name): Path<String>,
    State(state): State<Arc<AppState>>,
) -> Response {
    let room = state.get_or_create_room(&room_name);
    ws.on_upgrade(move |socket| handle_socket(socket, room_name, room))
}

async fn handle_socket(socket: WebSocket, room_name: String, room: Arc<Room>) {
    let user_id = Uuid::new_v4().to_string()[..8].to_string();
    let mut nick = format!("user_{user_id}");
    let mut rx = room.tx.subscribe();

    let (mut sender, mut receiver) = socket.split();

    // Уведомляем комнату о входе
    let join_msg = ChatMessage {
        room: room_name.clone(),
        author: "система".into(),
        text: format!("→ {nick} вошёл в комнату"),
    };
    let _ = room.tx.send(join_msg);

    info!("{nick} подключился к #{room_name}");

    let room_for_send = room.clone();
    let nick_for_send = nick.clone();
    let room_name_for_send = room_name.clone();

    // Задача: отправляем broadcast входящим клиентам
    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            let text = format!("[{}] {}: {}", msg.room, msg.author, msg.text);
            if sender.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    // Задача: читаем сообщения от клиента
    let mut recv_task = tokio::spawn(async move {
        let mut nick = nick_for_send;
        while let Some(Ok(msg)) = receiver.next().await {
            let text = match msg {
                Message::Text(t) => t.to_string(),
                Message::Close(_) => break,
                _ => continue,
            };

            let text = text.trim().to_owned();
            if text.is_empty() {
                continue;
            }

            // Команды
            if text.starts_with('/') {
                let parts: Vec<&str> = text.splitn(3, ' ').collect();
                match parts[0] {
                    "/nick" => {
                        if parts.len() >= 2 {
                            let old = nick.clone();
                            nick = parts[1].to_owned();
                            let _ = room_for_send.tx.send(ChatMessage {
                                room: room_name_for_send.clone(),
                                author: "система".into(),
                                text: format!("{old} сменил ник на {nick}"),
                            });
                        }
                    }
                    "/who" => {
                        // В реальном приложении тут был бы список подписчиков
                        let n = room_for_send.tx.receiver_count();
                        let _ = room_for_send.tx.send(ChatMessage {
                            room: room_name_for_send.clone(),
                            author: "система".into(),
                            text: format!("В комнате {n} участников"),
                        });
                    }
                    _ => {
                        let _ = room_for_send.tx.send(ChatMessage {
                            room: room_name_for_send.clone(),
                            author: "система".into(),
                            text: format!("Неизвестная команда: {}", parts[0]),
                        });
                    }
                }
                continue;
            }

            let chat_msg = ChatMessage {
                room: room_name_for_send.clone(),
                author: nick.clone(),
                text,
            };
            if room_for_send.tx.send(chat_msg).is_err() {
                break;
            }
        }
    });

    // Ждём завершения одной из задач
    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }

    let leave_msg = ChatMessage {
        room: room_name.clone(),
        author: "система".into(),
        text: format!("← {nick} покинул комнату"),
    };
    let _ = room.tx.send(leave_msg);
    info!("{nick} отключился от #{room_name}");
}

async fn index_handler() -> Html<&'static str> {
    Html(HTML_CLIENT)
}

// ─── HTML клиент ─────────────────────────────────────────────────────────────

const HTML_CLIENT: &str = r#"<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>wschat</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #0f0f0f; color: #e0e0e0; height: 100vh; display: flex; flex-direction: column; }
  #header { padding: .8rem 1rem; background: #1a1a1a; border-bottom: 1px solid #2a2a2a; display: flex; gap: .5rem; align-items: center; }
  #header h1 { font-size: 1rem; color: #58a6ff; flex: none; }
  #room-input { flex: 1; max-width: 180px; padding: .35rem .7rem; background: #111; border: 1px solid #333; border-radius: 4px; color: #e0e0e0; font-size: .9rem; }
  #connect-btn { padding: .35rem .9rem; background: #58a6ff; color: #000; border: none; border-radius: 4px; cursor: pointer; font-size: .9rem; font-weight: 600; }
  #status { font-size: .8rem; color: #666; }
  #messages { flex: 1; overflow-y: auto; padding: 1rem; display: flex; flex-direction: column; gap: .3rem; }
  .msg { font-size: .9rem; }
  .msg .author { color: #58a6ff; font-weight: 600; }
  .msg.system { color: #666; font-style: italic; }
  #input-row { display: flex; gap: .5rem; padding: .8rem 1rem; border-top: 1px solid #222; }
  #msg-input { flex: 1; padding: .5rem .9rem; background: #1a1a1a; border: 1px solid #333; border-radius: 4px; color: #e0e0e0; font-size: .95rem; }
  #send-btn { padding: .5rem 1rem; background: #238636; color: #fff; border: none; border-radius: 4px; cursor: pointer; font-size: .9rem; }
</style>
</head>
<body>
<div id="header">
  <h1>wschat</h1>
  <input id="room-input" type="text" value="general" placeholder="комната">
  <button id="connect-btn" onclick="connect()">Подключиться</button>
  <span id="status">не подключён</span>
</div>
<div id="messages"></div>
<div id="input-row">
  <input id="msg-input" type="text" placeholder="Сообщение... (/nick Имя, /who)" onkeydown="if(event.key==='Enter')send()">
  <button id="send-btn" onclick="send()">Отправить</button>
</div>
<script>
let ws = null;
const msgs = document.getElementById('messages');
const status = document.getElementById('status');

function addMsg(text, isSystem) {
  const div = document.createElement('div');
  div.className = 'msg' + (isSystem ? ' system' : '');
  div.textContent = text;
  msgs.appendChild(div);
  msgs.scrollTop = msgs.scrollHeight;
}

function connect() {
  const room = document.getElementById('room-input').value.trim() || 'general';
  if (ws) ws.close();
  const url = `ws://${location.host}/ws/${room}`;
  ws = new WebSocket(url);
  ws.onopen = () => { status.textContent = `#${room}`; status.style.color = '#3fb950'; };
  ws.onclose = () => { status.textContent = 'отключён'; status.style.color = '#666'; };
  ws.onmessage = e => {
    const isSystem = e.data.includes('система:');
    addMsg(e.data, isSystem);
  };
}

function send() {
  const inp = document.getElementById('msg-input');
  const text = inp.value.trim();
  if (!text || !ws || ws.readyState !== 1) return;
  ws.send(text);
  inp.value = '';
}

connect();
</script>
</body>
</html>"#;

// ─── main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "wschat=info".into())
                .as_str(),
        )
        .init();

    let cli = Cli::parse();
    let state = Arc::new(AppState::new(cli.buffer));

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/ws/{room}", get(ws_handler))
        .with_state(state);

    let addr = format!("{}:{}", cli.host, cli.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    info!("wschat слушает на http://{addr}");

    axum::serve(listener, app).await.unwrap();
}
