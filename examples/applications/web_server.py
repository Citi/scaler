import nest_asyncio
nest_asyncio.apply()

import sys
import grequests
import requests
import asyncio
from random import randint
from psutil import cpu_count
from timeit import default_timer as timer
from quart import Quart
from scaler import SchedulerClusterCombo, Client

def server(scaled: bool):
    app = Quart(__name__)

    if scaled:
        cluster = SchedulerClusterCombo(n_workers=cpu_count())
        client = Client(address=cluster.get_address())

        @app.route("/")
        async def root():
            future = client.submit(requests.get, "https://example.com")
            response: requests.Response = await asyncio.wrap_future(future)
            return response.text
    
        app.run(loop=asyncio.get_event_loop())
        cluster.shutdown()
    else:
        @app.route("/")
        def root():
            return requests.get("https://example.com").text
    
        app.run()


def client():
    start = timer()
    requests = [grequests.get("http://127.0.0.1:5000") for _ in range(1000)]
    responses = grequests.map(requests)
    print(f"Time: {timer() - start:.2f}s")

if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "server" and sys.argv[2] == "scaled":
        server(scaled=True)
    elif len(sys.argv) == 2 and sys.argv[1] == "server" and sys.argv[2] == "unscaled":
        server(scaled=False)
    elif len(sys.argv) == 2 and sys.argv[1] == "client":
        client()
    else:
        print("Usage: web_server.py server|client [scaled|unscaled]")
        sys.exit(1)
