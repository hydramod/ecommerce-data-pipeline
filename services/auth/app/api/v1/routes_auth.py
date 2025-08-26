from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.api.v1.schemas import (
    RegisterPayload,
    LoginPayload,
    TokenPair,
    RefreshRequest,
)
from app.db.models import User, RefreshToken
from app.security.utils import (
    hash_password,
    verify_password,
    create_access_token,
    create_refresh_token,
    token_sha256,
    now_utc,
    decode_token,
)

router = APIRouter()  # main.py mounts at /auth


@router.post("/register", status_code=status.HTTP_201_CREATED)
def register(payload: RegisterPayload, db: Session = Depends(get_db)) -> Any:
    # Prevent duplicate email
    if db.query(User).filter(User.email == str(payload.email)).first():
        raise HTTPException(status_code=409, detail="Email already registered")

    user = User(
        email=str(payload.email),
        password_hash=hash_password(payload.password),
        role=payload.role or "customer",
        created_at=now_utc(),
        updated_at=now_utc(),
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"id": user.id, "email": user.email, "role": user.role}


@router.post("/login", response_model=TokenPair)
def login(payload: LoginPayload, db: Session = Depends(get_db)) -> TokenPair:
    user = db.query(User).filter(User.email == str(payload.email)).first()
    if not user or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    access, _ = create_access_token(user.email, user.role)
    refresh, jti, exp = create_refresh_token(user.email)

    db.add(
        RefreshToken(
            user_id=user.id,
            jti=jti,
            token_hash=token_sha256(refresh),
            expires_at=exp,
            revoked=False,
            created_at=now_utc(),
        )
    )
    db.commit()

    return {"access_token": access, "refresh_token": refresh, "token_type": "bearer"}


@router.post("/refresh", response_model=TokenPair, status_code=status.HTTP_200_OK)
def refresh_token(payload: RefreshRequest, db: Session = Depends(get_db)) -> TokenPair:
    # 1) Decode & validate refresh token
    try:
        claims = decode_token(payload.refresh_token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    if claims.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid token type")

    jti = claims.get("jti")
    email = claims.get("sub")
    if not jti or not email:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    # 2) Verify token record (not revoked/expired and matches user)
    rt = (
        db.query(RefreshToken)
        .join(User)
        .filter(RefreshToken.jti == jti, User.email == email)
        .first()
    )
    if not rt or rt.revoked or rt.expires_at < now_utc():
        raise HTTPException(status_code=401, detail="Refresh token not valid")

    # 3) Revoke the used refresh token
    rt.revoked = True
    db.add(rt)

    # 4) Issue new access + refresh
    access_token, _ = create_access_token(email, rt.user.role)
    new_refresh, new_jti, new_exp = create_refresh_token(email)

    db.add(
        RefreshToken(
            user_id=rt.user_id,
            jti=new_jti,
            token_hash=token_sha256(new_refresh),
            expires_at=new_exp,
            revoked=False,
            created_at=now_utc(),
        )
    )
    db.commit()

    return TokenPair(access_token=access_token, refresh_token=new_refresh)


@router.post("/logout", status_code=status.HTTP_200_OK)
def logout(payload: RefreshRequest, db: Session = Depends(get_db)) -> dict:
    # 1) Decode & validate refresh token
    try:
        claims = decode_token(payload.refresh_token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    if claims.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="Invalid token type")

    jti = claims.get("jti")
    if not jti:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    # 2) Revoke it if present
    rt = db.query(RefreshToken).filter(RefreshToken.jti == jti).first()
    if rt and not rt.revoked:
        rt.revoked = True
        db.add(rt)
        db.commit()

    return {"status": "ok"}
