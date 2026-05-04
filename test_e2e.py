"""
test_e2e.py
-----------
Інтеграційний тест: запускає сервер, підключає трьох клієнтів,
надсилає кілька повідомлень і перевіряє, що broadcast працює.
"""
import subprocess, time, socket, sys, os, signal
from protocol import ChatMessage, FrameReader, Protocol

HOST, PORT = "127.0.0.1", 9876

def recv_msgs(sock, fr, timeout=1.0):
    sock.settimeout(timeout)
    msgs = []
    end = time.time() + timeout
    while time.time() < end:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                break
            msgs.extend(fr.feed(chunk))
        except socket.timeout:
            break
        except OSError:
            break
    return msgs

def main():
    # Запускаємо сервер
    server = subprocess.Popen(
        [sys.executable, "server.py", HOST, str(PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        cwd=os.path.dirname(os.path.abspath(__file__)) or ".",
        preexec_fn=os.setsid,
    )
    time.sleep(1.5)  # дати серверу запуститись

    clients = []
    readers = []
    try:
        # Підключаємо 3 клієнтів
        for name in ("Alice", "Bob", "Carol"):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((HOST, PORT))
            s.sendall(ChatMessage(Protocol.JOIN, name, "").to_bytes())
            clients.append((name, s))
            readers.append(FrameReader())
            time.sleep(0.2)

        time.sleep(0.5)
        # Очистимо стартові JOIN/SYS повідомлення
        for (_, s), fr in zip(clients, readers):
            recv_msgs(s, fr, 0.5)

        # Alice пише "Привіт всім!"
        clients[0][1].sendall(
            ChatMessage(Protocol.MESSAGE, "Alice", "Привіт всім!").to_bytes()
        )
        time.sleep(0.5)

        # Перевіряємо що Bob і Carol отримали
        success = True
        for (name, s), fr in zip(clients, readers):
            msgs = recv_msgs(s, fr, 0.5)
            got = any(m.content == "Привіт всім!" and m.sender == "Alice"
                      for m in msgs)
            print(f"[{name}] отримав broadcast: {got}; повідомлень: {len(msgs)}")
            if not got:
                success = False

        # Bob запитує /list
        clients[1][1].sendall(ChatMessage(Protocol.LIST, "Bob", "").to_bytes())
        time.sleep(0.4)
        msgs = recv_msgs(clients[1][1], readers[1], 0.5)
        for m in msgs:
            print(f"[Bob LIST] {m}")

        print("\n=== РЕЗУЛЬТАТ:", "OK" if success else "FAIL", "===")
        return 0 if success else 1
    finally:
        for _, s in clients:
            try: s.close()
            except OSError: pass
        # Вбиваємо групу процесів сервера (бо є дочірні процеси multiprocessing)
        try:
            os.killpg(os.getpgid(server.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        server.wait(timeout=3)

if __name__ == "__main__":
    sys.exit(main())
