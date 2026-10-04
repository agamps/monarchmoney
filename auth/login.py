import asyncio

from common.client import login

if __name__ == "__main__":
    asyncio.run(login(verbose=True))
