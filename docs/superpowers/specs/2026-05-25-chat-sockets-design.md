# Chat Multi-Usuario con Sockets TCP — Diseño

**Fecha:** 2026-05-25  
**Estado:** Aprobado

## Objetivo

Convertir el proyecto básico de sockets TCP (un cliente, un mensaje) en un chat multi-usuario con GUI tkinter en el cliente y servidor asyncio con logs en terminal.

## Archivos

```
sockets/
├── servidor.py    # asyncio server con logs en terminal
├── cliente.py     # GUI tkinter + hilo socket
└── README.md      # documentación rehecha
```

## Arquitectura

```
[cliente A tkinter] ──TCP──┐
[cliente B tkinter] ──TCP──┤── servidor.py (asyncio) ──► logs terminal
[cliente C tkinter] ──TCP──┘        │
                                     └── broadcast a todos los demás
```

### servidor.py

- `asyncio.start_server()` escucha en `0.0.0.0:5000`
- `clients: dict[asyncio.StreamWriter, str]` — mapea writer → username
- `handle_client(reader, writer)` — coroutine independiente por cliente:
  1. Lee primer mensaje `USERNAME:<nombre>`, valida no vacío y no duplicado
  2. Agrega a `clients`, broadcast `JOIN:<nombre>`
  3. Envía `USERS:<n1>,<n2>,...` a todos
  4. Loop: lee mensajes → broadcast `MSG:<nombre>:<texto>`
  5. En desconexión (`EOF` o excepción): elimina de `clients`, broadcast `LEAVE:<nombre>`, actualiza `USERS`
- `broadcast(msg: str, exclude=None)` — envía a todos los writers excepto excluido
- Logs en terminal con timestamp: `[CONNECT]`, `[MSG]`, `[DISCONNECT]`

### cliente.py

**Pantalla de login:**
- Campos: IP del servidor (default `127.0.0.1`), puerto (default `5000`), nombre de usuario
- Botón "Conectar" — valida campos no vacíos antes de conectar

**Ventana de chat:**
- Panel izquierdo (~75%): `Text` widget scrollable, read-only. Mensajes coloreados:
  - `JOIN`/`LEAVE` → gris itálica
  - Mensajes propios → azul
  - Mensajes ajenos → negro
- Panel derecho (~25%): `Listbox` con usuarios conectados en tiempo real
- Panel inferior: `Entry` + botón "Enviar". `Enter` también envía. Deshabilitado si no conectado.

**Concurrencia en cliente:**
- Hilo de fondo (`threading.Thread`, daemon=True): loop `socket.recv()` → pone strings en `queue.Queue`
- `root.after(100, check_queue)` en hilo principal: drena queue → actualiza widgets

**Manejo de errores:**
- Servidor: `try/except` en `handle_client` aísla fallos por cliente, nunca cae el servidor
- Cliente: si recv retorna vacío o excepción → mensaje "Desconectado del servidor" en chat, deshabilita Entry, muestra botón "Reconectar"
- Username duplicado: servidor rechaza con mensaje `ERROR:Username en uso`, cliente muestra diálogo y vuelve a login

## Protocolo (texto plano, UTF-8, `\n` como delimitador)

| Mensaje             | Dirección            | Significado                        |
|---------------------|----------------------|------------------------------------|
| `USERNAME:<nombre>` | cliente → servidor   | primer mensaje al conectar         |
| `MSG:<nombre>:<texto>` | servidor → clientes | mensaje broadcast                |
| `JOIN:<nombre>`     | servidor → clientes  | nuevo usuario conectado            |
| `LEAVE:<nombre>`    | servidor → clientes  | usuario desconectado               |
| `USERS:<n1>,<n2>`   | servidor → clientes  | lista completa de usuarios activos |
| `ERROR:<motivo>`    | servidor → cliente   | error (ej: username duplicado)     |

## Dependencias

Solo stdlib de Python 3.7+: `asyncio`, `tkinter`, `threading`, `queue`, `socket`

## Ejecución

```bash
# Terminal 1
python servidor.py

# Terminal 2, 3, 4... (distintas máquinas o la misma)
python cliente.py
```
