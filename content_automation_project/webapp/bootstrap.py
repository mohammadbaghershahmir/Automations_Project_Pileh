import json
import logging
import os

from sqlalchemy import func
from sqlalchemy.orm import Session

from webapp.auth_utils import hash_password
from webapp.models import User

logger = logging.getLogger(__name__)


def _admin_pairs_from_env():
    """Return [(email_lower, password_plain), ...] from ADMIN_BOOTSTRAP or ADMIN_EMAIL_1..3."""
    admins = []
    raw = os.environ.get("ADMIN_BOOTSTRAP")
    if raw:
        try:
            admins = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("ADMIN_BOOTSTRAP is not valid JSON")
            return []
    if not admins:
        for i in range(1, 4):
            email = os.environ.get(f"ADMIN_EMAIL_{i}")
            password = os.environ.get(f"ADMIN_PASSWORD_{i}")
            if email and password:
                admins.append({"email": email, "password": password})

    out = []
    for a in admins:
        email = a.get("email")
        password = a.get("password")
        if not email or not password:
            continue
        email_n = str(email).strip().lower()
        password_n = str(password).strip()
        if email_n and password_n:
            out.append((email_n, password_n))
    return out


def sync_admin_password_from_env(db: Session) -> None:
    """One-time recovery: set ADMIN_EMAIL_1's password from ADMIN_PASSWORD_1 when flag is set.

    Use when the DB was created with a wrong hash (e.g. Docker Compose mangled `$` in env).
    Remove SYNC_ADMIN_PASSWORD_FROM_ENV after a successful login and restart.
    """
    flag = os.environ.get("SYNC_ADMIN_PASSWORD_FROM_ENV", "").strip().lower()
    if flag not in ("1", "true", "yes", "on"):
        return
    email = os.environ.get("ADMIN_EMAIL_1", "").strip().lower()
    password = os.environ.get("ADMIN_PASSWORD_1", "").strip()
    if not email or not password:
        logger.warning(
            "SYNC_ADMIN_PASSWORD_FROM_ENV is set but ADMIN_EMAIL_1 or ADMIN_PASSWORD_1 is missing or empty"
        )
        return
    user = db.query(User).filter(func.lower(User.email) == email).first()
    if not user:
        logger.warning(
            "SYNC_ADMIN_PASSWORD_FROM_ENV: no user with email %s (case-insensitive match) — "
            "set ADMIN_EMAIL_1 to the same address you use to log in",
            email,
        )
        return
    user.password_hash = hash_password(password)
    db.commit()
    logger.warning(
        "SYNC_ADMIN_PASSWORD_FROM_ENV: password updated for %s — remove this env var and restart",
        email,
    )


def bootstrap_admins(db: Session) -> None:
    """Create admin users from env if table is empty."""
    if db.query(User).first() is not None:
        return

    pairs = _admin_pairs_from_env()
    if not pairs:
        logger.warning(
            "No admin users configured (set ADMIN_EMAIL_1 / ADMIN_PASSWORD_1 or ADMIN_BOOTSTRAP). "
            "Creating default admin@localhost / changeme — CHANGE IMMEDIATELY."
        )
        pairs = [("admin@localhost", "changeme")]

    for email_n, password_n in pairs:
        db.add(User(email=email_n, password_hash=hash_password(password_n)))
        logger.info("Bootstrapped admin user: %s", email_n)
    db.commit()


def ensure_missing_env_admins(db: Session) -> None:
    """Create any ADMIN_EMAIL_* / ADMIN_BOOTSTRAP accounts that are in env but not yet in the DB.

    Initial bootstrap only runs when the users table is empty, so admins 2/3 (or new emails in
    ADMIN_BOOTSTRAP) would otherwise never be created. Call this on every API startup after
    bootstrap_admins.
    """
    added = False
    for email_n, password_n in _admin_pairs_from_env():
        exists = db.query(User).filter(func.lower(User.email) == email_n).first()
        if exists:
            continue
        db.add(User(email=email_n, password_hash=hash_password(password_n)))
        logger.info("Added env admin user (was missing in DB): %s", email_n)
        added = True
    if added:
        db.commit()
