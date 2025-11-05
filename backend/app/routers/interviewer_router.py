from fastapi import APIRouter, HTTPException, Form
from typing import Optional
from sqlalchemy import select
from db import AsyncSessionLocal
from models import Interviewer
from schemas.interview_schema import (
    GetInterviewerRequest,
    UpdateInterviewerRequest,
    DeleteInterviewerRequest
)
import uuid

router = APIRouter(prefix="/api/interviewer", tags=["interviewer"])

async def get_interviewer_or_404(db, interviewer_id: str):
    result = await db.execute(
        select(Interviewer).where(Interviewer.id == uuid.UUID(interviewer_id))
    )
    interviewer = result.scalar_one_or_none()
    if not interviewer:
        raise HTTPException(status_code=404, detail="Interviewer not found")
    return interviewer

def serialize_interviewer(interviewer):
    return {
        "id": str(interviewer.id),
        "name": interviewer.name,
        "persona": interviewer.persona,
        "accent": interviewer.accent,
        "elevenlabs_voice_id": interviewer.elevenlabs_voice_id,
        "avatar_url": interviewer.avatar_url,
        "created_at": interviewer.created_at.isoformat() if interviewer.created_at else None,
    }

@router.post("/create-interviewer")
async def create_interviewer(
    name: str = Form(...),
    persona: Optional[str] = Form(None),
    accent: Optional[str] = Form(None),
    elevenlabs_voice_id: Optional[str] = Form(None),
    avatar_url: Optional[str] = Form(None),
):
    async with AsyncSessionLocal() as db:
        try:
            interviewer = Interviewer(
                name=name,
                persona=persona,
                accent=accent,
                elevenlabs_voice_id=elevenlabs_voice_id,
                avatar_url=avatar_url
            )
            db.add(interviewer)
            await db.commit()
            await db.refresh(interviewer)
            return serialize_interviewer(interviewer)
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to create interviewer: {str(e)}")

@router.get("/list-interviewers")
async def list_interviewers():
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(Interviewer).where(Interviewer.is_active == True).order_by(Interviewer.created_at.desc())
        )
        interviewers = result.scalars().all()
        return {
            "ok": True,
            "interviewers": [serialize_interviewer(i) for i in interviewers]
        }

@router.post("/get-interviewer")
async def get_interviewer(request: GetInterviewerRequest):
    async with AsyncSessionLocal() as db:
        interviewer = await get_interviewer_or_404(db, request.interviewer_id)
        return serialize_interviewer(interviewer)

@router.post("/update-interviewer")
async def update_interviewer(request: UpdateInterviewerRequest):
    async with AsyncSessionLocal() as db:
        interviewer = await get_interviewer_or_404(db, request.interviewer_id)
        
        if request.name is not None:
            interviewer.name = request.name
        if request.persona is not None:
            interviewer.persona = request.persona
        if request.accent is not None:
            interviewer.accent = request.accent
        if request.elevenlabs_voice_id is not None:
            interviewer.elevenlabs_voice_id = request.elevenlabs_voice_id
        if request.avatar_url is not None:
            interviewer.avatar_url = request.avatar_url
        if request.is_active is not None:
            interviewer.is_active = request.is_active

        try:
            await db.commit()
            await db.refresh(interviewer)
            return serialize_interviewer(interviewer)
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to update interviewer: {str(e)}")

@router.post("/delete-interviewer")
async def delete_interviewer(request: DeleteInterviewerRequest):
    async with AsyncSessionLocal() as db:
        interviewer = await get_interviewer_or_404(db, request.interviewer_id)
        try:
            await db.delete(interviewer)
            await db.commit()
            return {"ok": True, "message": "Interviewer deleted successfully"}
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to delete interviewer: {str(e)}")

