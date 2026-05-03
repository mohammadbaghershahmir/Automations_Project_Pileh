from typing import Annotated, Optional

from fastapi import Cookie, Depends, HTTPException, status
from sqlalchemy.orm import Session

from webapp.auth_utils import COOKIE_NAME, decode_token
from webapp.database import get_db
from webapp.models import User


def get_current_user(
    db: Annotated[Session, Depends(get_db)],
    access_token: Annotated[Optional[str], Cookie(alias=COOKIE_NAME)] = None,
) -> User:
    if not access_token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    payload = decode_token(access_token)
    if not payload or "uid" not in payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    user = db.query(User).filter(User.id == payload["uid"]).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User inactive")
    return user


CurrentUser = Annotated[User, Depends(get_current_user)]
