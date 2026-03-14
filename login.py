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


async def interactive_login_with_retry(max_attempts: int = 2) -> MonarchMoney:
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        mm = MonarchMoney()
        try:
            await mm.interactive_login()
            return mm
        except Exception as e:
            last_error = e
            if "401" not in str(e) or attempt == max_attempts:
                raise

            print("Interactive login returned 401. Retrying with a fresh client...")

    assert last_error is not None
    raise last_error


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

    mm = await interactive_login_with_retry()
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    mm.save_session(str(SESSION_FILE))
    print("Fresh session saved.")


if __name__ == "__main__":
    asyncio.run(main())
