"""
server.py
---------
Багатопроцесний TCP-чат-сервер.

Архітектура:
    * Головний процес  - приймає підключення (accept-loop).
    * Процес-broadcaster - читає глобальну чергу й роздає повідомлення
                           по індивідуальних чергах кожного клієнта.
    * Один процес на клієнта - власне з'єднання + дві потокові гілки
                               (читач/записувач сокета).

Демонструє:
    * TCP-сокети (модуль socket);
    * багатопроцесна обробка (multiprocessing.Process);
    * IPC через multiprocessing.Manager (Queue, dict);
    * принципи ООП — клас-сервер інкапсулює стан і поведінку.

Запуск:
    python3 server.py [host] [port]
    наприклад:  python3 server.py 0.0.0.0 9000
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import signal
import socket
import sys
import threading
import time
import uuid
from typing import Optional

from protocol import ChatMessage, FrameReader, Protocol


# ---------- Налаштування логування ----------
LOG_FORMAT = "%(asctime)s [%(processName)-15s] %(levelname)-7s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
log = logging.getLogger("ChatServer")


# =====================================================================
#                       КЛІЄНТСЬКИЙ ПРОЦЕС
# =====================================================================
def client_process(
    conn: socket.socket,
    addr: tuple,
    client_id: str,
    broadcast_queue: "mp.Queue",
    personal_queue: "mp.Queue",
    clients_registry: dict,
) -> None:
    """
    Обробник одного клієнта. Виконується в окремому процесі.

    Усередині процесу запускається 2 потоки:
        - reader_thread:  socket.recv() -> broadcast_queue
        - writer_thread:  personal_queue -> socket.send()

    Чому потоки всередині процесу, а не ще процеси?
        Тому що операції з сокетом блокуючі (recv) — нам потрібно одночасно
        читати й писати в один і той самий socket. Створювати ще процеси
        для дрібних задач — надмірно дорого. Це і є ілюстрація комбінації
        процесного й потокового паралелізму.
    """
    proc_log = logging.getLogger(f"Client-{client_id[:6]}")
    proc_log.info("Старт клієнтського процесу для %s:%d", *addr)

    username: Optional[str] = None
    stop_event = threading.Event()

    # ---------- Потік-читач: socket -> broadcast_queue ----------
    def reader() -> None:
        nonlocal username
        reader_buffer = FrameReader()
        try:
            while not stop_event.is_set():
                try:
                    chunk = conn.recv(Protocol.BUFFER_SIZE)
                except (ConnectionResetError, OSError):
                    break
                if not chunk:                     # сокет закритий клієнтом
                    break
                for msg in reader_buffer.feed(chunk):
                    # Перше повідомлення JOIN несе нікнейм
                    if msg.msg_type == Protocol.JOIN and username is None:
                        username = msg.sender or f"user-{client_id[:4]}"
                        clients_registry[client_id] = username
                        proc_log.info("Користувач '%s' зареєстрований", username)
                        broadcast_queue.put(
                            ChatMessage(Protocol.JOIN, username, "").to_dict()
                        )
                        # Привітальне системне повідомлення тільки цьому клієнту
                        personal_queue.put(
                            ChatMessage(
                                Protocol.SYSTEM,
                                "server",
                                f"Ласкаво просимо, {username}! "
                                f"У чаті: {len(clients_registry)} користувач(ів).",
                            ).to_dict()
                        )
                    elif msg.msg_type == Protocol.LIST:
                        names = ", ".join(sorted(clients_registry.values()))
                        personal_queue.put(
                            ChatMessage(
                                Protocol.SYSTEM, "server",
                                f"Зараз онлайн: {names}",
                            ).to_dict()
                        )
                    else:
                        # Звичайне повідомлення — ставимо в глобальну чергу
                        msg.sender = username or "anon"
                        broadcast_queue.put(msg.to_dict())
        except Exception as exc:
            proc_log.exception("Помилка у потоці-читачі: %s", exc)
        finally:
            stop_event.set()
            personal_queue.put(None)              # розблокувати writer
            proc_log.info("Reader thread завершено")

    # ---------- Потік-записувач: personal_queue -> socket ----------
    def writer() -> None:
        try:
            while not stop_event.is_set():
                try:
                    item = personal_queue.get(timeout=0.5)
                except Exception:
                    continue
                if item is None:                  # сигнал зупинки
                    break
                try:
                    msg = ChatMessage.from_dict(item)
                    conn.sendall(msg.to_bytes())
                except (BrokenPipeError, ConnectionResetError, OSError):
                    break
        except Exception as exc:
            proc_log.exception("Помилка у потоці-записувачі: %s", exc)
        finally:
            stop_event.set()
            proc_log.info("Writer thread завершено")

    t_read = threading.Thread(target=reader, name="reader", daemon=True)
    t_write = threading.Thread(target=writer, name="writer", daemon=True)
    t_read.start()
    t_write.start()
    t_read.join()
    t_write.join()

    # ---------- Прибирання ----------
    if username:
        broadcast_queue.put(
            ChatMessage(Protocol.LEAVE, username, "").to_dict()
        )
    clients_registry.pop(client_id, None)
    try:
        conn.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    conn.close()
    proc_log.info("Клієнтський процес завершено")


# =====================================================================
#                       ПРОЦЕС-BROADCASTER
# =====================================================================
def broadcaster_process(
    broadcast_queue: "mp.Queue",
    client_queues: dict,
    clients_registry: dict,
    shutdown: "mp.Event",
) -> None:
    """
    Окремий процес, що читає глобальну чергу broadcast_queue й
    розкидає кожне повідомлення по індивідуальних чергах усіх клієнтів.

    Розв'язує задачу "fan-out" у багатопроцесному середовищі.
    """
    bc_log = logging.getLogger("Broadcaster")
    bc_log.info("Broadcaster запущено (pid=%d)", os.getpid())

    try:
        while not shutdown.is_set():
            try:
                item = broadcast_queue.get(timeout=0.5)
            except Exception:
                continue
            if item is None:
                break
            msg = ChatMessage.from_dict(item)
            bc_log.info("→ broadcast: %s", msg)

            # Знімок ключів — безпечно ітеруватися навіть під час змін
            for cid in list(client_queues.keys()):
                q = client_queues.get(cid)
                if q is None:
                    continue
                try:
                    q.put(item)
                except Exception:
                    # Черга могла бути вже знищена після виходу клієнта
                    pass
    finally:
        bc_log.info("Broadcaster зупинено")


# =====================================================================
#                       ОСНОВНИЙ КЛАС СЕРВЕРА
# =====================================================================
class ChatServer:
    """
    Серверний клас, що інкапсулює сокет, реєстр клієнтів і життєвий цикл.

    Параметри
    ---------
    host : str
        IP-адреса для прив'язки. '0.0.0.0' — слухати всі інтерфейси.
    port : int
        Номер TCP-порту.
    backlog : int
        Розмір черги очікуючих з'єднань (listen backlog).
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 9000,
                 backlog: int = 16) -> None:
        self.host = host
        self.port = port
        self.backlog = backlog

        # ---------- Менеджер для shared-об'єктів ----------
        # Manager — це окремий процес-сервер, який створює проксі-об'єкти
        # та забезпечує синхронізацію між основним і дочірніми процесами.
        self._manager = mp.Manager()
        self._broadcast_queue: mp.Queue = self._manager.Queue()
        self._client_queues: dict = self._manager.dict()
        self._clients_registry: dict = self._manager.dict()
        self._shutdown = self._manager.Event()

        self._broadcaster: Optional[mp.Process] = None
        self._client_procs: list[mp.Process] = []
        self._listener: Optional[socket.socket] = None

    # ---------- Запуск ----------
    def start(self) -> None:
        """Точка входу. Запускає broadcaster і accept-loop."""
        self._setup_signal_handlers()

        self._broadcaster = mp.Process(
            target=broadcaster_process,
            name="Broadcaster",
            args=(self._broadcast_queue, self._client_queues,
                  self._clients_registry, self._shutdown),
        )
        self._broadcaster.start()

        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR — щоб після перезапуску не чекати TIME_WAIT
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((self.host, self.port))
        self._listener.listen(self.backlog)
        self._listener.settimeout(1.0)            # щоб accept() був перерваним

        log.info("Сервер слухає %s:%d (PID=%d)", self.host, self.port, os.getpid())

        try:
            self._accept_loop()
        finally:
            self.stop()

    # ---------- Цикл прийому з'єднань ----------
    def _accept_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                conn, addr = self._listener.accept()
            except socket.timeout:
                # Чергова перевірка стану shutdown
                self._reap_dead_processes()
                continue
            except OSError:
                break

            client_id = uuid.uuid4().hex
            personal_q: mp.Queue = self._manager.Queue()
            self._client_queues[client_id] = personal_q

            proc = mp.Process(
                target=client_process,
                name=f"Client-{client_id[:6]}",
                args=(conn, addr, client_id,
                      self._broadcast_queue, personal_q,
                      self._clients_registry),
            )
            proc.start()
            # Сокет тепер належить дочірньому процесу — у батьківському
            # ми його закриваємо, щоб дескриптор звільнився.
            conn.close()
            self._client_procs.append(proc)
            log.info("Нове з'єднання: %s:%d -> client_id=%s (proc=%d)",
                     addr[0], addr[1], client_id[:6], proc.pid)

    # ---------- Допоміжне ----------
    def _reap_dead_processes(self) -> None:
        """Прибирає завершені дочірні процеси."""
        alive = []
        for p in self._client_procs:
            if p.is_alive():
                alive.append(p)
            else:
                p.join(timeout=0.1)
                log.debug("Похований дочірній процес %s", p.name)
        self._client_procs = alive

    def _setup_signal_handlers(self) -> None:
        def handler(signum, frame):
            log.warning("Отримано сигнал %s — зупиняюся…", signum)
            self._shutdown.set()
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    # ---------- Зупинка ----------
    def stop(self) -> None:
        """Коректно завершує всі дочірні процеси."""
        log.info("Завершення роботи…")
        self._shutdown.set()

        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass

        # Сигнал на завершення broadcaster-у
        try:
            self._broadcast_queue.put(None)
        except Exception:
            pass

        for p in self._client_procs:
            p.join(timeout=2.0)
            if p.is_alive():
                p.terminate()

        if self._broadcaster is not None:
            self._broadcaster.join(timeout=2.0)
            if self._broadcaster.is_alive():
                self._broadcaster.terminate()

        log.info("Сервер зупинено.")


# =====================================================================
#                                MAIN
# =====================================================================
def main() -> None:
    host = sys.argv[1] if len(sys.argv) > 1 else "0.0.0.0"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000

    # На Linux/macOS використовується fork — найшвидший варіант,
    # який дозволяє прозоро передавати об'єкт socket дочірньому процесу.
    # На Windows доступний лише spawn — тоді сокет потрібно
    # серіалізувати окремо, але для академічних цілей цього достатньо.
    if sys.platform != "win32":
        mp.set_start_method("fork", force=True)

    server = ChatServer(host=host, port=port)
    server.start()


if __name__ == "__main__":
    main()
