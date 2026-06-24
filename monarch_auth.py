"""Shared Monarch authentication helpers.

Provides a single async entrypoint to obtain an authenticated
`MonarchMoney` client, handling saved-session loading, validation,
and automatic interactive re-login when needed.

Placeholders:
 - Expects `login.py` in the same directory to create the session file.
 - Session file path: `.mm/mm_session.pickle` (keeps existing behaviour).
"""
from pathlib import Path
import subprocess
import sys

from gql.transport.exceptions import TransportServerError
from monarchmoney import MonarchMoney

SESSION_FILE = Path(".mm/mm_session.pickle")
LOGIN_SCRIPT = Path("login.py")


def _run_login_script() -> None:
    result = subprocess.run([sys.executable, str(LOGIN_SCRIPT)], check=False)
    if result.returncode != 0:
        raise RuntimeError(f"{LOGIN_SCRIPT} failed with exit code {result.returncode}")


async def get_monarch_client() -> MonarchMoney:
    """Return an authenticated MonarchMoney client.

    Will run `login.py` to create a saved session file if none exists,
    and will re-run it automatically if a loaded session proves invalid (401).
    """
    if not SESSION_FILE.exists():
        _run_login_script()
        if not SESSION_FILE.exists():
            raise RuntimeError(
                f"{LOGIN_SCRIPT} completed but did not create {SESSION_FILE}."
            )

    mm = MonarchMoney()
    mm.load_session(str(SESSION_FILE))

    try:
        await mm.get_accounts()
        return mm
    except TransportServerError as e:
        if "401" not in str(e):
            raise

        # Session expired; attempt interactive re-login
        _run_login_script()
        if not SESSION_FILE.exists():
            raise RuntimeError(
                "Monarch re-login completed but no session file was saved."
            )

        mm = MonarchMoney()
        mm.load_session(str(SESSION_FILE))
        await mm.get_accounts()
        return mm
