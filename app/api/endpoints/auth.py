"""Auth endpoint — upserts a Clerk user into the Neon DB on first login."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models.models import User
from app.schemas.schemas import UserSyncRequest, UserResponse

router = APIRouter()


@router.post("/sync", response_model=UserResponse, status_code=status.HTTP_200_OK)
async def sync_user(
    payload: UserSyncRequest,
    db: AsyncSession = Depends(get_db),
) -> UserResponse:
    """
    Upsert a user record from Clerk into the application database.

    Called once after login from the frontend. Idempotent — safe to call multiple times.
    """
    result = await db.execute(select(User).where(User.clerk_id == payload.clerk_id))
    user = result.scalar_one_or_none()

    if user is None:
        user = User(
            clerk_id=payload.clerk_id,
            email=payload.email,
            full_name=payload.full_name,
            avatar_url=payload.avatar_url,
        )
        db.add(user)
    else:
        # Update mutable fields on subsequent syncs
        user.email = payload.email
        if payload.full_name is not None:
            user.full_name = payload.full_name
        if payload.avatar_url is not None:
            user.avatar_url = payload.avatar_url

    await db.commit()
    await db.refresh(user)
    return UserResponse.model_validate(user)
