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
        await asyncio.sleep(0.1)

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
