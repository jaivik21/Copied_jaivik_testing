from fastapi import APIRouter, HTTPException
from datetime import datetime, timezone
from sqlalchemy import update, func, select
import uuid
from db import AsyncSessionLocal
from models import Interview, Response
from schemas.interview_schema import StartInterviewRequest, EndInterviewRequest
from utils.interview_utils import get_interview_or_404, get_response_or_404, commit_and_refresh, get_questions_list
from utils.redis_utils import create_session, set_session_meta
from services.llm_service import llm_service
import secrets

router = APIRouter(prefix="/api/interview", tags=["sessions"])

@router.post("/start-interview")
async def start_interview(request: StartInterviewRequest):
    async with AsyncSessionLocal() as db:
        interview = await get_interview_or_404(db, request.interview_id)
        
        if not interview.is_open:
            raise HTTPException(status_code=403, detail="This interview is currently closed. Please contact the HR team.")
        
        email = request.candidate_email.lower().strip() if request.candidate_email else None
        
        if email:
            existing_responses = await db.execute(
                select(Response)
                .where(Response.interview_id == interview.id)
                .where(Response.email == email)
            )
            existing = existing_responses.scalars().first()
            if existing:
                raise HTTPException(
                    status_code=403,
                    detail="You have already completed this interview. Each candidate can only take the interview once."
                )
            
            if interview.respondents and len(interview.respondents) > 0:
                if email not in [r.lower().strip() for r in interview.respondents]:
                    raise HTTPException(
                        status_code=403,
                        detail="Your email address is not authorized to take this interview. Please contact the HR team if you believe this is an error."
                    )
        
        response = Response(
            interview_id=interview.id,
            name=request.candidate_name,
            email=email or request.candidate_email,
            start_time=datetime.now(timezone.utc)
        )
        db.add(response)
        await commit_and_refresh(db, response)
        
        await db.execute(
            update(Interview)
            .where(Interview.id == interview.id)
            .values(response_count=func.coalesce(Interview.response_count, 0) + 1)
        )
        await db.commit()

        session_id = f"ws_{interview.id}_{response.id}"
        session_token = secrets.token_urlsafe(24)
        try:
            await create_session(session_id)
            await set_session_meta(session_id, {
                "interview_id": str(interview.id),
                "response_id": str(response.id),
                "mode": interview.question_mode,
                "session_token": session_token,
                "started_at": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            print(f"[ERROR] Redis session init failed: {e}")

        duration_minutes = None
        if interview.time_duration and interview.time_duration.isdigit():
            duration_minutes = int(interview.time_duration)
        
        return {
            "ok": True,
            "response_id": str(response.id),
            "interview_id": str(interview.id),
            "session_id": session_id,
            "session_token": session_token,
            "mode": interview.question_mode,
            "duration_minutes": duration_minutes,
            "start_time": response.start_time.isoformat() if response.start_time else None
        }

@router.post("/end-interview")
async def end_interview(request: EndInterviewRequest):
    async with AsyncSessionLocal() as db:
        response = await get_response_or_404(db, request.response_id)
        interview = await get_interview_or_404(db, str(response.interview_id))
        
        response.is_completed = True
        
        end_time = datetime.now(timezone.utc)
        if not response.end_time:
            response.end_time = end_time
        elif response.end_time < end_time:
            response.end_time = end_time
        
        if response.start_time and response.end_time:
            duration_delta = response.end_time - response.start_time
            duration_seconds = int(duration_delta.total_seconds())
            response.duration = duration_seconds
            print(f"[DEBUG] Interview duration calculated: {duration_seconds} seconds ({duration_seconds // 60}m {duration_seconds % 60}s)")
        
        qa_history = response.qa_history or []
        if len(qa_history) > 0:
            overall_analysis = getattr(response, "overall_analysis", None)
            if not overall_analysis:
                try:
                    if interview.context:
                        final_analysis = await llm_service.generate_final_analysis(
                            str(interview.id), qa_history
                        )
                        try:
                            setattr(response, "overall_analysis", final_analysis)
                            
                            if final_analysis and (not hasattr(response, 'status') or not response.status or response.status == "no_status"):
                                score = final_analysis.get("overall_score", 0)
                                if score >= 80:
                                    response.status = "selected"
                                elif score >= 60:
                                    response.status = "potential"
                                elif score < 40:
                                    response.status = "not_selected"
                                else:
                                    response.status = "potential"
                                response.status_source = "auto"
                        except Exception as e:
                            print(f"[WARN] Failed to set overall_analysis: {e}")
                except Exception as e:
                    print(f"[WARN] Final analysis generation failed: {e}")
        
        await db.commit()
        
        if interview.question_mode == "dynamic":
            total_questions = interview.question_count or 0
        else:
            questions_list = get_questions_list(interview)
            total_questions = len(questions_list) if questions_list else 0
        
        questions_answered = len(qa_history)
        is_partially_complete = questions_answered < total_questions if total_questions > 0 else False
        
        return {
            "ok": True,
            "message": "Interview ended successfully",
            "questions_answered": questions_answered,
            "total_questions": total_questions,
            "is_partially_complete": is_partially_complete,
            "end_time": response.end_time.isoformat() if response.end_time else None,
            "duration_seconds": response.duration if response.duration else None
        }

