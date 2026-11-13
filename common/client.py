"""Shared Monarch authentication helpers.

`get_monarch_client()` returns an authenticated `MonarchMoney` client,
loading the saved session, validating it, and falling back to an
interactive login when it is missing or expired.

Session file: see `common.config.session_file()`
(default ~/monarch-data/.mm/mm_session.pickle).
"""
import getpass

from aiohttp import ClientSession
from gql.transport.exceptions import TransportServerError
from monarchmoney import LoginFailedException, MonarchMoney, MonarchMoneyEndpoints, RequireMFAException

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


async def request_token(mm: MonarchMoney, email: str, password: str, code: str | None = None) -> None:
    """The library's login request, but as a trusted device.

    The library always sends trusted_device=False, and Monarch then expires the
    session about 30 minutes after login.
    """
    data = {"username": email, "password": password, "supports_mfa": True, "trusted_device": True}
    if code:
        data["totp"] = code
    async with ClientSession(headers=mm._headers) as session:
        async with session.post(MonarchMoneyEndpoints.getLoginEndpoint(), json=data) as resp:
            if resp.status == 403 and not code:
                raise RequireMFAException("Multi-Factor Auth Required")
            try:
                body = await resp.json()
            except Exception:
                body = {}
            if resp.status != 200:
                reason = body.get("detail") or body.get("error_code") or f"HTTP {resp.status}: {resp.reason}"
                raise LoginFailedException(reason)
    mm.set_token(body["token"])
    mm._headers["Authorization"] = f"Token {body['token']}"


async def prompt_login(mm: MonarchMoney) -> None:
    """Like the library's interactive_login, but trusted, and a mistyped MFA code can be re-entered."""
    email = input("Email: ")
    password = getpass.getpass("Password: ")
    try:
        await request_token(mm, email, password)
        return
    except RequireMFAException:
        pass

    for attempt in range(1, MFA_CODE_ATTEMPTS + 1):
        code = input("Two Factor Code: ").strip()
        try:
            await request_token(mm, email, password, code)
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

    try:
        mm = await interactive_login_with_retry()
    except EOFError:
        # No terminal to type into (e.g. input piped or closed).
        raise SystemExit("Login needed: run auth/login.py in a terminal, then try again.") from None
    path.parent.mkdir(parents=True, exist_ok=True)
    mm.save_session(str(path))
    print(f"Fresh session saved to {path}.")
    return mm


async def get_monarch_client() -> MonarchMoney:
    """Return an authenticated MonarchMoney client."""
    return await login()
