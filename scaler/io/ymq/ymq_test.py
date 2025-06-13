import asyncio
import ymq

context = ymq.IOContext(num_threads=2)
socket = context.createIOSocket("my-socket", ymq.IOSocketType.Dealer)

print(context, ";", socket)

async def main():
    await socket.send()

asyncio.run(main())
