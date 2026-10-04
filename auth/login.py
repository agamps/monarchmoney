import asyncio

from common.auth import login

if __name__ == "__main__":
    asyncio.run(login(verbose=True))
