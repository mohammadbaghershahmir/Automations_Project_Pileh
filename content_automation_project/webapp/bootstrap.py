import json
import logging
import os

from sqlalchemy.orm import Session

from webapp.auth_utils import hash_password
from webapp.models import User

logger = logging.getLogger(__name__)


def bootstrap_admins(db: Session) -> None:
    """Create admin users from env if table is empty."""
    if db.query(User).first() is not None:
        return

    admins = []
    raw = os.environ.get("ADMIN_BOOTSTRAP")
    if raw:
        try:
            admins = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("ADMIN_BOOTSTRAP is not valid JSON")
    if not admins:
        for i in range(1, 4):
            email = os.environ.get(f"ADMIN_EMAIL_{i}")
            password = os.environ.get(f"ADMIN_PASSWORD_{i}")
            if email and password:
                admins.append({"email": email, "password": password})

    if not admins:
        logger.warning(
            "No admin users configured (set ADMIN_EMAIL_1 / ADMIN_PASSWORD_1 or ADMIN_BOOTSTRAP). "
            "Creating default admin@localhost / changeme — CHANGE IMMEDIATELY."
        )
        admins = [{"email": "admin@localhost", "password": "changeme"}]

    for a in admins:
        email = a.get("email")
        password = a.get("password")
        if not email or not password:
            continue
        db.add(User(email=email.strip().lower(), password_hash=hash_password(password)))
        logger.info("Bootstrapped admin user: %s", email)
    db.commit()
