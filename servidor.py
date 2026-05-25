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
