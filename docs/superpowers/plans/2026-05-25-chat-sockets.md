# Chat Multi-Usuario con Sockets TCP — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reemplazar el proyecto básico de sockets con un chat multi-usuario: servidor asyncio con logs en terminal, cliente con GUI tkinter, protocolo de texto plano sobre TCP.

**Architecture:** El servidor usa `asyncio.start_server` con una coroutine por cliente y un dict global `clients: dict[StreamWriter, str]` para broadcast. El cliente usa tkinter en el hilo principal + un hilo de fondo para `socket.recv`, conectados por `queue.Queue` + `root.after(100, check_queue)`.

**Tech Stack:** Python 3.8+ stdlib solamente: `asyncio`, `tkinter`, `threading`, `queue`, `socket`, `unittest.IsolatedAsyncioTestCase`

---

## Mapa de archivos

| Archivo | Acción | Responsabilidad |
|---|---|---|
| `servidor.py` | Reemplazar | Servidor asyncio, broadcast, logs terminal |
| `cliente.py` | Reemplazar | GUI tkinter + hilo socket + queue |
| `tests/__init__.py` | Crear | Marca directorio como package |
| `tests/test_servidor.py` | Crear | Tests de integración asyncio para servidor |
| `README.md` | Reemplazar | Documentación completa |

---

## Task 1: Estructura de tests y stub de servidor

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/test_servidor.py`
- Modify: `servidor.py` (stub mínimo para que tests puedan importarlo)

- [ ] **Step 1: Crear `tests/__init__.py` vacío**

```bash
touch tests/__init__.py
```

- [ ] **Step 2: Crear stub mínimo de `servidor.py`**

Reemplazar el contenido de `servidor.py` con:

```python
import asyncio

HOST = "0.0.0.0"
PORT = 5000

clients: dict = {}  # StreamWriter -> username


async def broadcast(msg: str, exclude=None):
    pass


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    pass


async def main():
    pass


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 3: Escribir `tests/test_servidor.py`**

```python
import asyncio
import sys
import unittest

sys.path.insert(0, ".")
import servidor


class TestServidor(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        servidor.clients.clear()
        self.server = await asyncio.start_server(
            servidor.handle_client, "127.0.0.1", 5001
        )

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()
        servidor.clients.clear()

    async def _connect(self, username: str):
        reader, writer = await asyncio.open_connection("127.0.0.1", 5001)
        writer.write(f"USERNAME:{username}\n".encode())
        await writer.drain()
        return reader, writer

    async def test_connect_receives_users(self):
        """Al conectar, el cliente recibe USERS: con su propio nombre."""
        reader, writer = await self._connect("Alice")
        line = await asyncio.wait_for(reader.readline(), timeout=2)
        msg = line.decode().strip()
        self.assertTrue(msg.startswith("USERS:"), f"Esperaba USERS:, recibí: {msg}")
        self.assertIn("Alice", msg)
        writer.close()
        await writer.wait_closed()

    async def test_duplicate_username_rejected(self):
        """Nombre duplicado debe recibir ERROR: y la conexión se cierra."""
        r1, w1 = await self._connect("Alice")
        await asyncio.wait_for(r1.readline(), timeout=2)  # USERS:Alice

        r2, w2 = await self._connect("Alice")
        line = await asyncio.wait_for(r2.readline(), timeout=2)
        msg = line.decode().strip()
        self.assertTrue(msg.startswith("ERROR:"), f"Esperaba ERROR:, recibí: {msg}")

        w1.close()
        await w1.wait_closed()
        try:
            w2.close()
            await w2.wait_closed()
        except Exception:
            pass

    async def test_broadcast_message(self):
        """Mensaje de Alice llega a Bob como MSG:Alice:<texto>."""
        r1, w1 = await self._connect("Alice")
        await asyncio.wait_for(r1.readline(), timeout=2)  # USERS:Alice

        r2, w2 = await self._connect("Bob")
        await asyncio.sleep(0.1)  # dar tiempo al servidor para procesar join

        # Drenar mensajes pendientes de Alice: JOIN:Bob y USERS:Alice,Bob
        await asyncio.wait_for(r1.readline(), timeout=1)  # JOIN:Bob
        await asyncio.wait_for(r1.readline(), timeout=1)  # USERS:Alice,Bob
        # Drenar mensajes pendientes de Bob: USERS:Alice,Bob
        await asyncio.wait_for(r2.readline(), timeout=1)

        # Alice envía mensaje
        w1.write("Hola!\n".encode())
        await w1.drain()

        # Bob debe recibir MSG:Alice:Hola!
        line = await asyncio.wait_for(r2.readline(), timeout=2)
        self.assertEqual(line.decode().strip(), "MSG:Alice:Hola!")

        w1.close()
        w2.close()
        await asyncio.gather(w1.wait_closed(), w2.wait_closed(),
                             return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 4: Ejecutar tests — verificar que FALLAN**

```bash
python -m pytest tests/test_servidor.py -v
```

Resultado esperado: 3 tests FAILED (el stub no implementa nada).

- [ ] **Step 5: Commit del stub y tests**

```bash
git add tests/__init__.py tests/test_servidor.py servidor.py
git commit -m "test: add asyncio integration tests for servidor (failing)"
```

---

## Task 2: Implementar `servidor.py`

**Files:**
- Modify: `servidor.py`

- [ ] **Step 1: Reemplazar `servidor.py` con implementación completa**

```python
import asyncio
import datetime

HOST = "0.0.0.0"
PORT = 5000

clients: dict = {}  # StreamWriter -> username


def _ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")


async def broadcast(msg: str, exclude=None):
    data = (msg + "\n").encode()
    for writer in list(clients):
        if writer is exclude:
            continue
        try:
            writer.write(data)
            await writer.drain()
        except Exception:
            pass


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    username = None
    try:
        raw = await asyncio.wait_for(reader.readline(), timeout=10)
        data = raw.decode().strip()

        if not data.startswith("USERNAME:"):
            writer.close()
            return

        username = data[9:].strip()

        if not username:
            writer.write("ERROR:Nombre vacío\n".encode())
            await writer.drain()
            writer.close()
            return

        if username in clients.values():
            writer.write("ERROR:Username en uso\n".encode())
            await writer.drain()
            writer.close()
            return

        clients[writer] = username
        print(f"[{_ts()}][CONNECT] {username} desde {addr}")

        await broadcast(f"JOIN:{username}", exclude=writer)
        await broadcast(f"USERS:{','.join(clients.values())}")

        async for raw_line in reader:
            text = raw_line.decode().strip()
            if text:
                print(f"[{_ts()}][MSG] {username} → todos: {text}")
                await broadcast(f"MSG:{username}:{text}")

    except asyncio.IncompleteReadError:
        pass
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        print(f"[{_ts()}][ERROR] {e}")
    finally:
        if username and writer in clients:
            del clients[writer]
            print(f"[{_ts()}][DISCONNECT] {username}")
            await broadcast(f"LEAVE:{username}")
            remaining = ",".join(clients.values())
            if remaining:
                await broadcast(f"USERS:{remaining}")
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    addr = server.sockets[0].getsockname()
    print(f"[{_ts()}][SERVIDOR] Escuchando en {addr[0]}:{addr[1]}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SERVIDOR] Detenido.")
```

- [ ] **Step 2: Ejecutar tests — verificar que PASAN**

```bash
python -m pytest tests/test_servidor.py -v
```

Resultado esperado:
```
PASSED tests/test_servidor.py::TestServidor::test_broadcast_message
PASSED tests/test_servidor.py::TestServidor::test_connect_receives_users
PASSED tests/test_servidor.py::TestServidor::test_duplicate_username_rejected
3 passed
```

- [ ] **Step 3: Commit**

```bash
git add servidor.py
git commit -m "feat: implement asyncio multi-user chat server"
```

---

## Task 3: Implementar `cliente.py`

**Files:**
- Modify: `cliente.py`

- [ ] **Step 1: Reemplazar `cliente.py` con implementación completa**

```python
import socket
import threading
import queue
import tkinter as tk
from tkinter import scrolledtext, messagebox

BUFFER = 4096


class ChatClient:
    def __init__(self):
        self.sock = None
        self.recv_queue: queue.Queue = queue.Queue()
        self.username = ""
        self.connected = False

        self.root = tk.Tk()
        self.root.title("Chat Sockets")
        self.root.resizable(False, False)
        self._build_login()
        self.root.mainloop()

    # ── Login ──────────────────────────────────────────────────────────────

    def _build_login(self):
        self.root.geometry("320x230")
        frame = tk.Frame(self.root, padx=20, pady=20)
        frame.pack(expand=True)

        tk.Label(frame, text="Chat Sockets", font=("Arial", 16, "bold")).grid(
            row=0, columnspan=2, pady=(0, 15)
        )

        fields = [
            ("IP Servidor:", "127.0.0.1", "ip_var"),
            ("Puerto:",      "5000",      "port_var"),
            ("Nombre:",      "",          "name_var"),
        ]
        for i, (label, default, attr) in enumerate(fields, start=1):
            tk.Label(frame, text=label).grid(row=i, column=0, sticky="e", pady=4)
            var = tk.StringVar(value=default)
            setattr(self, attr, var)
            entry = tk.Entry(frame, textvariable=var, width=18)
            entry.grid(row=i, column=1, sticky="w")
            if attr == "name_var":
                entry.focus()
                entry.bind("<Return>", lambda e: self._connect())

        tk.Button(
            frame, text="Conectar", command=self._connect,
            width=20, bg="#4CAF50", fg="white", font=("Arial", 10, "bold"),
        ).grid(row=4, columnspan=2, pady=(15, 0))

    def _connect(self):
        ip = self.ip_var.get().strip()
        port_str = self.port_var.get().strip()
        username = self.name_var.get().strip()

        if not ip or not port_str or not username:
            messagebox.showerror("Error", "Todos los campos son requeridos")
            return

        try:
            port = int(port_str)
        except ValueError:
            messagebox.showerror("Error", "Puerto debe ser un número")
            return

        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((ip, port))
            self.sock.sendall(f"USERNAME:{username}\n".encode())

            self.sock.settimeout(5.0)
            try:
                data = self.sock.recv(BUFFER).decode("utf-8", errors="replace")
            finally:
                self.sock.settimeout(None)

            lines = [l.strip() for l in data.split("\n") if l.strip()]
            if not lines:
                raise ConnectionError("Sin respuesta del servidor")
            if lines[0].startswith("ERROR:"):
                messagebox.showerror("Error", lines[0][6:])
                self.sock.close()
                self.sock = None
                return

            self.username = username
            self.connected = True
            self._build_chat()

            for line in lines:
                self._process_message(line)

            threading.Thread(target=self._recv_loop, daemon=True).start()
            self.root.after(100, self._check_queue)

        except Exception as e:
            messagebox.showerror("Error de conexión", str(e))
            if self.sock:
                self.sock.close()
                self.sock = None

    # ── Chat UI ────────────────────────────────────────────────────────────

    def _build_chat(self):
        for w in self.root.winfo_children():
            w.destroy()

        self.root.geometry("720x520")
        self.root.resizable(True, True)
        self.root.title(f"Chat — {self.username}")

        top = tk.Frame(self.root)
        top.pack(fill="both", expand=True, padx=8, pady=(8, 0))

        self.msg_area = scrolledtext.ScrolledText(
            top, state="disabled", wrap="word",
            font=("Consolas", 10), cursor="arrow",
        )
        self.msg_area.pack(side="left", fill="both", expand=True)
        self.msg_area.tag_config("system", foreground="#888888",
                                  font=("Consolas", 10, "italic"))
        self.msg_area.tag_config("own",    foreground="#1565C0",
                                  font=("Consolas", 10, "bold"))
        self.msg_area.tag_config("other",  foreground="#212121")

        right = tk.Frame(top, width=150, relief="sunken", bd=1)
        right.pack(side="right", fill="y", padx=(6, 0))
        right.pack_propagate(False)
        tk.Label(right, text="Conectados", font=("Arial", 10, "bold"),
                 bg="#E3F2FD").pack(fill="x")
        self.users_list = tk.Listbox(right, font=("Arial", 10), bd=0,
                                      selectbackground="#BBDEFB")
        self.users_list.pack(fill="both", expand=True)

        bottom = tk.Frame(self.root)
        bottom.pack(fill="x", padx=8, pady=8)
        self.msg_entry = tk.Entry(bottom, font=("Consolas", 11))
        self.msg_entry.pack(side="left", fill="x", expand=True)
        self.msg_entry.bind("<Return>", lambda e: self._send())
        self.msg_entry.focus()
        tk.Button(
            bottom, text="Enviar", command=self._send,
            bg="#1565C0", fg="white", width=10, font=("Arial", 10, "bold"),
        ).pack(side="right", padx=(6, 0))

    # ── Networking ─────────────────────────────────────────────────────────

    def _send(self):
        text = self.msg_entry.get().strip()
        if not text or not self.connected:
            return
        try:
            self.sock.sendall((text + "\n").encode())
            self._append_message(f"[Tú]: {text}", "own")
            self.msg_entry.delete(0, "end")
        except Exception:
            self._on_disconnect()

    def _recv_loop(self):
        buf = ""
        try:
            while True:
                chunk = self.sock.recv(BUFFER)
                if not chunk:
                    break
                buf += chunk.decode("utf-8", errors="replace")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    if line.strip():
                        self.recv_queue.put(line.strip())
        except Exception:
            pass
        finally:
            self.recv_queue.put("__DISCONNECTED__")

    def _check_queue(self):
        try:
            while True:
                msg = self.recv_queue.get_nowait()
                if msg == "__DISCONNECTED__":
                    self._on_disconnect()
                    return
                self._process_message(msg)
        except queue.Empty:
            pass
        self.root.after(100, self._check_queue)

    def _process_message(self, msg: str):
        if msg.startswith("MSG:"):
            parts = msg[4:].split(":", 1)
            if len(parts) == 2:
                sender, text = parts
                self._append_message(f"[{sender}]: {text}", "other")
        elif msg.startswith("JOIN:"):
            self._append_message(f"  {msg[5:]} se unió al chat", "system")
        elif msg.startswith("LEAVE:"):
            self._append_message(f"  {msg[6:]} salió del chat", "system")
        elif msg.startswith("USERS:"):
            users = [u for u in msg[6:].split(",") if u]
            self.users_list.delete(0, "end")
            for u in users:
                self.users_list.insert("end", f"  {u}")
        elif msg.startswith("ERROR:"):
            messagebox.showerror("Error del servidor", msg[6:])

    def _append_message(self, text: str, tag: str):
        self.msg_area.config(state="normal")
        self.msg_area.insert("end", text + "\n", tag)
        self.msg_area.see("end")
        self.msg_area.config(state="disabled")

    def _on_disconnect(self):
        self.connected = False
        if hasattr(self, "msg_entry"):
            self.msg_entry.config(state="disabled")
            self._append_message("  Desconectado del servidor.", "system")


if __name__ == "__main__":
    ChatClient()
```

- [ ] **Step 2: Commit**

```bash
git add cliente.py
git commit -m "feat: add tkinter chat client with threading socket bridge"
```

---

## Task 4: Reescribir `README.md`

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Reemplazar `README.md`**

```markdown
# Chat Multi-Usuario con Sockets TCP en Python

Chat en tiempo real sobre TCP puro. Servidor asíncrono con `asyncio`, interfaz gráfica con `tkinter`. Sin dependencias externas.

## Cómo funciona

```
[Cliente A] ──TCP──┐
[Cliente B] ──TCP──┤── servidor.py (asyncio) ──► logs en terminal
[Cliente C] ──TCP──┘
```

El servidor hace **broadcast**: cada mensaje recibido se reenvía a todos los demás clientes conectados. El servidor puede manejar múltiples clientes simultáneamente mediante `asyncio` (un event loop, sin threads en el servidor).

El cliente usa `tkinter` para la GUI. Un hilo de fondo hace `socket.recv` en loop y deposita mensajes en una `queue.Queue`. El hilo principal de tkinter drena esa queue cada 100 ms con `root.after()`.

## Protocolo

Mensajes de texto plano separados por `\n`, codificados en UTF-8.

| Mensaje              | Dirección           | Significado                    |
|----------------------|---------------------|--------------------------------|
| `USERNAME:<nombre>`  | cliente → servidor  | primer mensaje al conectar     |
| `MSG:<nombre>:<txt>` | servidor → clientes | mensaje broadcast              |
| `JOIN:<nombre>`      | servidor → clientes | nuevo usuario conectado        |
| `LEAVE:<nombre>`     | servidor → clientes | usuario desconectado           |
| `USERS:<n1>,<n2>`    | servidor → clientes | lista actualizada de usuarios  |
| `ERROR:<motivo>`     | servidor → cliente  | error (ej: username duplicado) |

## Uso

**Terminal 1 — arrancar el servidor:**
```bash
python servidor.py
```

**Terminal 2, 3, 4... — abrir clientes (misma máquina o red local):**
```bash
python cliente.py
```

En la pantalla de login: ingresar IP del servidor, puerto (`5000`) y nombre de usuario. Clic en **Conectar**.

## Tests

```bash
python -m pytest tests/ -v
```

Los tests levantan un servidor en `127.0.0.1:5001` y verifican: recepción de lista de usuarios al conectar, rechazo de username duplicado, y broadcast de mensajes entre clientes.

## Requisitos

Python 3.8+ — solo stdlib, sin `pip install`.

## Estructura

```
sockets/
├── servidor.py              # servidor asyncio, logs en terminal
├── cliente.py               # GUI tkinter + hilo socket
├── tests/
│   ├── __init__.py
│   └── test_servidor.py     # tests de integración asyncio
└── README.md
```

## Tecnologías

| Módulo | Uso |
|---|---|
| `asyncio` | event loop del servidor, una coroutine por cliente |
| `tkinter` | GUI del cliente (stdlib, sin instalación) |
| `threading` | hilo de fondo en cliente para `socket.recv` |
| `queue.Queue` | puente thread-safe entre hilo socket y tkinter |
| `socket` | conexión TCP en el cliente |
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README with architecture, protocol, and usage"
```

---

## Task 5: Prueba de integración manual

- [ ] **Step 1: Ejecutar todos los tests unitarios**

```bash
python -m pytest tests/ -v
```

Resultado esperado: 3 passed, 0 failed.

- [ ] **Step 2: Arrancar servidor en una terminal**

```bash
python servidor.py
```

Resultado esperado:
```
[HH:MM:SS][SERVIDOR] Escuchando en 0.0.0.0:5000
```

- [ ] **Step 3: Abrir dos clientes en terminales separadas**

```bash
python cliente.py
```

Conectar con nombre "Alice" en el primero, "Bob" en el segundo. Verificar:
- La lista "Conectados" muestra ambos usuarios
- Al enviar mensaje desde Alice, Bob lo recibe (y viceversa)
- Los mensajes propios aparecen en azul, ajenos en negro
- Al cerrar un cliente, el otro recibe notificación de salida y la lista se actualiza

- [ ] **Step 4: Verificar username duplicado**

Abrir tercer cliente e intentar conectar con nombre "Alice" (ya conectado). Debe aparecer diálogo de error.

- [ ] **Step 5: Commit final si todo funciona**

```bash
git add -A
git commit -m "chore: finalize multi-user chat project"
```
