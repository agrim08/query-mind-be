import logging
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.design import GenerateSchemaRequest, DBSchemaDesign
from app.services.schema_generator import generate_schema_from_prompt
from app.api import deps
from app.db.session import get_db
from app.models.models import User, DesignLog

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/generate-schema", response_model=DBSchemaDesign)
async def generate_schema(
    request: GenerateSchemaRequest,
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Generates a structured database schema and saves it to the user's design history.
    """
    try:
        schema = await generate_schema_from_prompt(request.prompt)
        
        # Save to history
        log = DesignLog(
            user_id=current_user.id,
            prompt=request.prompt,
            schema_json=schema.model_dump()
        )
        db.add(log)
        await db.commit()
        
        return schema
    except Exception as e:
        logger.error(f"Error generating schema: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history")
async def get_design_history(
    current_user: User = Depends(deps.get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Retrieves the design history for the current user.
    """
    try:
        result = await db.execute(
            select(DesignLog)
            .where(DesignLog.user_id == current_user.id)
            .order_by(DesignLog.created_at.desc())
        )
        return result.scalars().all()
    except Exception as e:
        logger.error(f"Error fetching design history: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not fetch design history")
