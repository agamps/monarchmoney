"""Shared Monarch authentication helpers.

`get_monarch_client()` returns an authenticated `MonarchMoney` client,
loading the saved session, validating it, and falling back to an
interactive login when it is missing or expired.

Session file: see `common.config.session_file()`
(default ~/monarch-data/.mm/mm_session.pickle).
"""
from gql.transport.exceptions import TransportServerError
from monarchmoney import MonarchMoney

from common.config import session_file
from common.monarch_api import configure_monarch_api

configure_monarch_api()


async def session_is_valid(mm: MonarchMoney) -> bool:
    try:
        # Cheap authenticated call to verify the session still works
        await mm.get_accounts()
        return True
    except TransportServerError as e:
        if "401" in str(e):
            return False
        raise
    except TimeoutError:
        print("Timed out validating saved session.")
        return False


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


async def login(verbose: bool = False) -> MonarchMoney:
    """Load a valid saved session, or log in interactively and save one."""
    path = session_file()

    if path.exists():
        if verbose:
            print(f"Found saved session: {path}")
        mm = MonarchMoney()
        mm.load_session(str(path))

        if await session_is_valid(mm):
            if verbose:
                print("Saved session is still valid.")
            return mm

        print("Saved session is invalid or expired. Refreshing login...")
        path.unlink(missing_ok=True)

    mm = await interactive_login_with_retry()
    path.parent.mkdir(parents=True, exist_ok=True)
    mm.save_session(str(path))
    print(f"Fresh session saved to {path}.")
    return mm


async def get_monarch_client() -> MonarchMoney:
    """Return an authenticated MonarchMoney client."""
    return await login()
