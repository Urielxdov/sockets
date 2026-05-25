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
