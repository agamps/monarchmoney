"""Shared Monarch authentication helpers.

`get_monarch_client()` returns an authenticated `MonarchMoney` client,
loading the saved session, validating it, and falling back to an
interactive login when it is missing or expired.

Session file: see `common.config.session_file()`
(default ~/monarch-data/.mm/mm_session.pickle).
"""
import getpass

from gql.transport.exceptions import TransportServerError
from monarchmoney import LoginFailedException, MonarchMoney, RequireMFAException

from common.config import session_file
from common.monarch_api import configure_monarch_api

configure_monarch_api()

MFA_CODE_ATTEMPTS = 3


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


def login_error_message(error: BaseException) -> str:
    """Monarch's reason for a failed login, without the library's raw HTTP dump."""
    # The library re-raises Monarch's reason (e.g. "Your code was invalid...") as a
    # LoginFailedException holding the raw response; the reason is earlier in the chain.
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, RequireMFAException) and str(current) != "Multi-Factor Auth Required":
            return str(current)
        current = current.__context__
    return str(error).splitlines()[0]


async def prompt_login(mm: MonarchMoney) -> None:
    """Like the library's interactive_login, but a mistyped MFA code can be re-entered."""
    email = input("Email: ")
    password = getpass.getpass("Password: ")
    try:
        await mm.login(email, password, use_saved_session=False, save_session=False)
        return
    except RequireMFAException:
        pass

    for attempt in range(1, MFA_CODE_ATTEMPTS + 1):
        code = input("Two Factor Code: ").strip()
        try:
            await mm.multi_factor_authenticate(email, password, code)
            return
        except (LoginFailedException, RequireMFAException) as e:
            if attempt == MFA_CODE_ATTEMPTS:
                raise
            print(login_error_message(e))


async def interactive_login_with_retry(max_attempts: int = 2) -> MonarchMoney:
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        # Pass the session path explicitly: the library otherwise loads/saves
        # .mm/mm_session.pickle relative to the current directory.
        mm = MonarchMoney(session_file=str(session_file()))
        try:
            await prompt_login(mm)
            return mm
        except Exception as e:
            last_error = e
            if "401" not in str(e) or attempt == max_attempts:
                if isinstance(e, (LoginFailedException, RequireMFAException)):
                    raise SystemExit(f"Login failed: {login_error_message(e)}") from None
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
        mm = MonarchMoney(session_file=str(path))
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
