import asyncio
from pathlib import Path

from gql.transport.exceptions import TransportServerError
from monarchmoney import MonarchMoney

SESSION_FILE = Path(".mm/mm_session.pickle")


async def session_is_valid(mm: MonarchMoney) -> bool:
    try:
        # Cheap authenticated call to verify the session still works
        await mm.get_accounts()
        return True
    except TransportServerError as e:
        if "401" in str(e):
            return False
        raise


async def main():
    mm = MonarchMoney()

    if SESSION_FILE.exists():
        print(f"Found saved session: {SESSION_FILE}")
        mm.load_session(str(SESSION_FILE))

        if await session_is_valid(mm):
            print("Saved session is still valid.")
            return

        print("Saved session is invalid or expired. Refreshing login...")
        SESSION_FILE.unlink(missing_ok=True)

    await mm.interactive_login()
    mm.save_session()
    print("Fresh session saved.")


if __name__ == "__main__":
    asyncio.run(main())
