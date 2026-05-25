# Chat Multi-Usuario con Sockets TCP en Python

Chat en tiempo real sobre TCP puro. Servidor asíncrono con `asyncio`, interfaz gráfica con `tkinter`. Sin dependencias externas — solo Python 3.8+.

## Cómo funciona

```
[Cliente A] ──TCP──┐
[Cliente B] ──TCP──┤── servidor.py (asyncio) ──► logs en terminal
[Cliente C] ──TCP──┘
```

El servidor hace **broadcast**: cada mensaje recibido se reenvía a todos los demás clientes. Maneja múltiples clientes simultáneamente con `asyncio` — un solo event loop, sin threads en el servidor.

El cliente usa **tkinter** para la GUI. Un hilo de fondo hace `socket.recv` en loop y deposita mensajes en una `queue.Queue`. El hilo principal de tkinter drena esa queue cada 100 ms con `root.after()`.

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
python3 servidor.py
```

**Terminal 2, 3, 4... — abrir clientes (misma máquina o red local):**
```bash
python3 cliente.py
```

En la pantalla de login: ingresar IP del servidor, puerto (`5000`) y nombre de usuario. Clic en **Conectar**.

## Tests

```bash
python3 -m pytest tests/ -v
# o sin pytest:
python3 -m unittest tests.test_servidor -v
```

Los tests levantan un servidor en `127.0.0.1:5001` y verifican:
- Al conectar, el cliente recibe la lista de usuarios activos
- Nombre duplicado es rechazado con `ERROR:`
- Mensajes de un cliente llegan a los demás como `MSG:<nombre>:<texto>`

## Requisitos

Python 3.8+ — solo stdlib. `tkinter` incluido en la mayoría de instalaciones Python (en Linux: `sudo apt install python3-tk` si no está).

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
| `tkinter` | GUI del cliente (stdlib) |
| `threading` | hilo de fondo en cliente para `socket.recv` |
| `queue.Queue` | puente thread-safe entre hilo socket y tkinter |
| `socket` | conexión TCP en el cliente |
