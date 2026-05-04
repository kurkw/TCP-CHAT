"""
protocol.py
-----------
Спільний модуль для клієнта та сервера.
Описує протокол прикладного рівня (Application-layer protocol)
поверх TCP та містить класи ChatMessage і Protocol.

Формат повідомлення: JSON-об'єкт, серіалізований у байти,
завершується роздільником b'\\n' для коректного фреймінгу
у потоковому TCP-каналі.

Приклад:
    {"type": "MSG", "sender": "Alice",
     "content": "Привіт!", "timestamp": "2026-04-27T12:00:00"}\\n
"""

from __future__ import annotations

import json
import datetime
from typing import Optional


class Protocol:
    """
    Константи та утиліти для серіалізації/десеріалізації повідомлень.

    Використовується принцип ООП "інкапсуляція" - усі деталі формату
    приховані всередині класу й доступні через статичні методи.
    """

    # ===== Технічні параметри =====
    ENCODING: str = "utf-8"          # Кодування рядків — універсальне для UA
    BUFFER_SIZE: int = 4096           # Розмір буфера читання сокета (4 КБ)
    DELIMITER: bytes = b"\n"          # Роздільник кадрів у TCP-потоці

    # ===== Типи повідомлень прикладного протоколу =====
    JOIN: str = "JOIN"        # Клієнт під'єднався
    LEAVE: str = "LEAVE"      # Клієнт від'єднався
    MESSAGE: str = "MSG"      # Звичайне користувацьке повідомлення
    SYSTEM: str = "SYS"       # Системне сповіщення від сервера
    ERROR: str = "ERR"        # Повідомлення про помилку
    LIST: str = "LIST"        # Запит/відповідь зі списком користувачів

    # ===== Команди клієнтського CLI =====
    CMD_QUIT: str = "/quit"
    CMD_LIST: str = "/list"
    CMD_HELP: str = "/help"

    @staticmethod
    def encode(msg_type: str, sender: str, content: str) -> bytes:
        """
        Серіалізує повідомлення у байти з роздільником.

        :param msg_type:  тип повідомлення (Protocol.MESSAGE, JOIN, …)
        :param sender:    ім'я відправника
        :param content:   текст повідомлення
        :return:          bytes готові до відправки через socket.send
        """
        payload = {
            "type": msg_type,
            "sender": sender,
            "content": content,
            "timestamp": datetime.datetime.now().isoformat(),
        }
        # ensure_ascii=False — щоб кирилиця не екранувалася як \uXXXX
        raw = json.dumps(payload, ensure_ascii=False).encode(Protocol.ENCODING)
        return raw + Protocol.DELIMITER

    @staticmethod
    def decode(raw: bytes) -> dict:
        """Десеріалізує один кадр у словник."""
        return json.loads(raw.decode(Protocol.ENCODING))


class ChatMessage:
    """
    Об'єктно-орієнтоване представлення одного повідомлення чату.

    Демонструє принципи ООП:
        * інкапсуляція — атрибути обернені у клас;
        * абстракція    — to_bytes()/from_dict() ховають формат;
        * поліморфізм   — перевизначений __str__ для зручного логу.
    """

    __slots__ = ("msg_type", "sender", "content", "timestamp")

    def __init__(
        self,
        msg_type: str,
        sender: str,
        content: str,
        timestamp: Optional[datetime.datetime] = None,
    ) -> None:
        self.msg_type: str = msg_type
        self.sender: str = sender
        self.content: str = content
        self.timestamp: datetime.datetime = timestamp or datetime.datetime.now()

    # ---------- Серіалізація ----------
    def to_bytes(self) -> bytes:
        """Перетворює об'єкт у байти, готові до відправки."""
        return Protocol.encode(self.msg_type, self.sender, self.content)

    def to_dict(self) -> dict:
        return {
            "type": self.msg_type,
            "sender": self.sender,
            "content": self.content,
            "timestamp": self.timestamp.isoformat(),
        }

    # ---------- Десеріалізація ----------
    @classmethod
    def from_dict(cls, data: dict) -> "ChatMessage":
        ts_raw = data.get("timestamp")
        ts = (
            datetime.datetime.fromisoformat(ts_raw)
            if ts_raw
            else datetime.datetime.now()
        )
        return cls(
            msg_type=data.get("type", Protocol.MESSAGE),
            sender=data.get("sender", "unknown"),
            content=data.get("content", ""),
            timestamp=ts,
        )

    @classmethod
    def from_bytes(cls, raw: bytes) -> "ChatMessage":
        return cls.from_dict(Protocol.decode(raw))

    # ---------- Поліморфізм ----------
    def __str__(self) -> str:
        time_str = self.timestamp.strftime("%H:%M:%S")
        if self.msg_type == Protocol.SYSTEM:
            return f"[{time_str}] *** {self.content} ***"
        if self.msg_type == Protocol.JOIN:
            return f"[{time_str}] >>> {self.sender} приєднався до чату"
        if self.msg_type == Protocol.LEAVE:
            return f"[{time_str}] <<< {self.sender} покинув чат"
        if self.msg_type == Protocol.ERROR:
            return f"[{time_str}] [ERROR] {self.content}"
        return f"[{time_str}] {self.sender}: {self.content}"

    def __repr__(self) -> str:
        return (
            f"ChatMessage(type={self.msg_type!r}, sender={self.sender!r}, "
            f"content={self.content[:30]!r})"
        )


class FrameReader:
    """
    Допоміжний клас для читання кадрів повідомлень із TCP-потоку.

    TCP — це потоковий протокол: байти з різних send() можуть
    злипнутися в одному recv() або, навпаки, прийти частинами.
    Тому необхідно явно виділяти повідомлення за роздільником '\\n'.
    """

    def __init__(self) -> None:
        self._buffer: bytes = b""

    def feed(self, chunk: bytes) -> list[ChatMessage]:
        """
        Додає прийнятий шматок до внутрішнього буфера й повертає
        усі повністю отримані повідомлення.
        """
        self._buffer += chunk
        messages: list[ChatMessage] = []

        while Protocol.DELIMITER in self._buffer:
            raw, self._buffer = self._buffer.split(Protocol.DELIMITER, 1)
            if not raw:
                continue
            try:
                messages.append(ChatMessage.from_bytes(raw))
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Пропускаємо пошкоджений кадр, не валимо з'єднання
                continue
        return messages
