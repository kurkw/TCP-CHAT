"""
client.py
---------
Консольний TCP-чат-клієнт.

Архітектура:
    * Головний потік - читає stdin, відправляє у сокет.
    * Потік-receiver  - читає сокет, друкує отримані повідомлення.

Запуск:
    python3 client.py [host] [port] [username]
    Приклад: python3 client.py 127.0.0.1 9000 Alice
"""

from __future__ import annotations

import logging
import socket
import sys
import threading
import time
from typing import Optional

from protocol import ChatMessage, FrameReader, Protocol


logging.basicConfig(level=logging.WARNING,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("ChatClient")


HELP_TEXT = """\
Доступні команди:
  /list  - показати онлайн-користувачів
  /help  - показати цю довідку
  /quit  - вийти з чату
"""


class ChatClient:
    """ООП-обгортка для клієнта чату."""

    def __init__(self, host: str, port: int, username: str) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.sock: Optional[socket.socket] = None
        self._reader = FrameReader()
        self._stop = threading.Event()
        self._receiver: Optional[threading.Thread] = None

    # ---------- Підключення ----------
    def connect(self) -> None:
        """Встановлює TCP-з'єднання та надсилає JOIN."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        # Відправляємо JOIN з ніком — сервер ним зареєструє користувача
        join = ChatMessage(Protocol.JOIN, self.username, "")
        self.sock.sendall(join.to_bytes())
        print(f"[*] Під'єднано до {self.host}:{self.port} як «{self.username}»")
        print(HELP_TEXT)

    # ---------- Потік прийому повідомлень ----------
    def _recv_loop(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    chunk = self.sock.recv(Protocol.BUFFER_SIZE)
                except (ConnectionResetError, OSError):
                    break
                if not chunk:
                    print("\n[*] З'єднання закрите сервером.")
                    break
                for msg in self._reader.feed(chunk):
                    # Друкуємо повідомлення, але не дублюємо власні MSG —
                    # сервер їх ретранслює всім, включно з відправником,
                    # тож користувач бачить, що сервер прийняв його репліку.
                    print(str(msg))
        finally:
            self._stop.set()

    # ---------- Цикл вводу ----------
    def _input_loop(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    line = input()
                except EOFError:
                    break
                if not line.strip():
                    continue

                if line == Protocol.CMD_QUIT:
                    print("[*] Вихід…")
                    break
                if line == Protocol.CMD_HELP:
                    print(HELP_TEXT)
                    continue
                if line == Protocol.CMD_LIST:
                    msg = ChatMessage(Protocol.LIST, self.username, "")
                    self.sock.sendall(msg.to_bytes())
                    continue

                msg = ChatMessage(Protocol.MESSAGE, self.username, line)
                try:
                    self.sock.sendall(msg.to_bytes())
                except (BrokenPipeError, OSError):
                    print("[!] Розрив з'єднання.")
                    break
        finally:
            self._stop.set()

    # ---------- Запуск ----------
    def run(self) -> None:
        self.connect()
        self._receiver = threading.Thread(
            target=self._recv_loop, name="receiver", daemon=True
        )
        self._receiver.start()
        try:
            self._input_loop()
        except KeyboardInterrupt:
            print("\n[*] Ctrl+C — вихід.")
        finally:
            self.shutdown()

    # ---------- Завершення ----------
    def shutdown(self) -> None:
        self._stop.set()
        if self.sock is not None:
            try:
                # Сповіщаємо сервер про намір вийти (LEAVE)
                bye = ChatMessage(Protocol.LEAVE, self.username, "")
                self.sock.sendall(bye.to_bytes())
            except OSError:
                pass
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.sock.close()
        if self._receiver is not None:
            self._receiver.join(timeout=1.0)


# =====================================================================
#                                MAIN
# =====================================================================
def main() -> None:
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    username = sys.argv[3] if len(sys.argv) > 3 else f"user{int(time.time()) % 1000}"

    client = ChatClient(host, port, username)
    try:
        client.run()
    except ConnectionRefusedError:
        print(f"[!] Не можу під'єднатися до {host}:{port}. "
              "Чи запущений сервер?")
        sys.exit(1)


if __name__ == "__main__":
    main()
