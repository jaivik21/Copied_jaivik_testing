from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from typing import Optional
from pathlib import Path
import uuid
from sqlalchemy import select, func
from sqlalchemy.orm.attributes import flag_modified
from db import AsyncSessionLocal
from models import Interview, Response
from schemas.interview_schema import (
    InterviewResponse,
    GetInterviewRequest,
    DeleteInterviewRequest,
    ToggleInterviewStatusRequest,
    ListInterviewResponsesRequest,
    UpdateInterviewRequest
)
from services.summarization_service import summarization_service
from services.llm_service import llm_service
from utils.interview_utils import (
    get_interview_or_404,
    extract_text_from_file,
    get_questions_list,
    parse_manual_questions,
    commit_and_refresh,
    format_duration
)

router = APIRouter(prefix="/api/interview", tags=["interview"])

def serialize_interview(interview):
    questions = get_questions_list(interview)
    candidate_link = None
    if interview.readable_slug:
        candidate_link = f"/candidate/interview/{interview.readable_slug}"
    elif interview.url:
        candidate_link = interview.url
    else:
        candidate_link = f"/candidate/interview/{interview.id}"
    
    return InterviewResponse(
        id=str(interview.id),
        name=interview.name,
        objective=interview.objective,
        mode=interview.question_mode,
        question_count=interview.question_count,
        context=interview.context,
        questions=questions if questions else None,
        candidate_link=candidate_link,
        description=interview.description,
        is_open=interview.is_open if hasattr(interview, 'is_open') else True,
    )

@router.post("/create-interview", response_model=InterviewResponse)
async def create_interview(
    name: str = Form(...),
    objective: str = Form(...),
    mode: str = Form(...),
    question_count: int = Form(...),
    auto_question_generate: bool = Form(...),
    manual_questions: str = Form(...),
    difficulty_level: Optional[str] = Form("medium"),  
    interviewer_id: Optional[str] = Form(None),
    duration_minutes: Optional[int] = Form(None),
    respondents: Optional[str] = Form(None),
    jd_file: UploadFile = File(None),
):
    async with AsyncSessionLocal() as db:
        if difficulty_level not in ["low", "medium", "high"]:
            difficulty_level = "medium"
        
        interviewer_id_uuid = None
        if interviewer_id:
            try:
                from models import Interviewer
                interviewer_id_uuid = uuid.UUID(interviewer_id)
                result = await db.execute(
                    select(Interviewer).where(Interviewer.id == interviewer_id_uuid)
                )
                interviewer = result.scalar_one_or_none()
                if not interviewer:
                    raise HTTPException(status_code=404, detail="Interviewer not found")
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid interviewer_id format")
        
        time_duration_str = None
        if duration_minutes and duration_minutes > 0:
            time_duration_str = str(duration_minutes)
        
        respondents_list = []
        if respondents and respondents.strip():
            try:
                # Support both newline-separated and comma-separated emails
                emails = respondents.replace(',', '\n').split('\n')
                respondents_list = [e.strip().lower() for e in emails if e.strip() and '@' in e.strip()]
            except:
                pass
        
        url_id = str(uuid.uuid4())
        url = f"/candidate/interview/{url_id}"
        
        readable_slug = None
        if name:
            readable_slug = name.lower().replace(' ', '-').replace('_', '-')
            readable_slug = ''.join(c for c in readable_slug if c.isalnum() or c == '-')[:50]
            readable_slug = readable_slug.strip('-')
        
        interview = Interview(
            name=name,
            objective=objective,
            question_mode=mode,
            question_count=question_count,
            auto_question_generate=auto_question_generate,
            manual_questions=parse_manual_questions(manual_questions),
            interviewer_id=interviewer_id_uuid,
            time_duration=time_duration_str,
            respondents=respondents_list if respondents_list else None,
            url=url,
            readable_slug=readable_slug
        )
        db.add(interview)
        await commit_and_refresh(db, interview)
        
        if not interview.context:
            interview.context = {}
        interview.context["difficulty_level"] = difficulty_level
        if "context_summary" not in interview.context:
            interview.context["context_summary"] = f"Interview objective: {objective}"
        flag_modified(interview, 'context')  
        
        if jd_file:
            allowed_extensions = ['.pdf', '.docx', '.doc', '.txt']
            file_extension = Path(jd_file.filename).suffix.lower()
            if file_extension not in allowed_extensions:
                raise HTTPException(status_code=400, detail=f"Unsupported file type. Allowed: {', '.join(allowed_extensions)}")
            
            jd_text = extract_text_from_file(await jd_file.read(), jd_file.filename)
            jd_summary = await summarization_service.summarize_jd(jd_text)
            if isinstance(jd_summary, dict):
                jd_summary["difficulty_level"] = difficulty_level
                jd_summary["context_summary"] = summarization_service.get_context_for_llm(jd_summary)
            interview.context = jd_summary
            flag_modified(interview, 'context')  
        
        await db.commit()
        
        if auto_question_generate:
            try:
                context_for_llm = summarization_service.get_context_for_llm(interview.context) if interview.context else ""
                
                if mode == "predefined":
                    result = await llm_service._generate_predefined_questions(
                        context_for_llm, question_count, difficulty_level, objective, name
                    )
                    generated_description = result.get('description', '')
                    
                    questions = result.get('questions', [])
                    if questions:
                        interview.llm_generated_questions = {"questions": questions[:question_count]}
                        flag_modified(interview, 'llm_generated_questions')
                    
                    if generated_description:
                        interview.description = generated_description
                        flag_modified(interview, 'description')
                elif mode == "dynamic":
                    result = await llm_service._generate_predefined_questions(
                        context_for_llm, 1, difficulty_level, objective, name
                    )
                    generated_description = result.get('description', '')
                    if generated_description:
                        interview.description = generated_description
                        flag_modified(interview, 'description')
                
                await db.commit()
            except Exception as e:
                print(f"[WARN] Failed to auto-generate description: {e}")

        return serialize_interview(interview)

@router.get("/list-interviews")
async def list_interviews():
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(Interview))
        interviews = result.scalars().all()
        items = []
        for it in interviews:
            count_result = await db.execute(
                select(Response)
                .where(Response.interview_id == it.id)
                .where(Response.is_completed == True)
            )
            completed_responses = count_result.scalars().all()
            actual_response_count = sum(
                1 for r in completed_responses
                if (r.qa_history and len(r.qa_history) > 0) or getattr(r, 'overall_analysis', None) is not None
            )
            
            items.append({
                "id": str(it.id),
                "name": it.name,
                "mode": it.question_mode,
                "question_count": it.question_count,
                "responses_count": actual_response_count,
                "is_open": it.is_open if hasattr(it, 'is_open') else True
            })
        return {"ok": True, "interviews": items}

@router.post("/get-interview", response_model=InterviewResponse)
async def get_interview(request: GetInterviewRequest):
    async with AsyncSessionLocal() as db:
        interview_id = request.interview_id
        
        try:
            interview_uuid = uuid.UUID(interview_id)
            interview = await get_interview_or_404(db, str(interview_uuid))
        except ValueError:
            result = await db.execute(
                select(Interview).where(Interview.readable_slug == interview_id)
            )
            interview = result.scalar_one_or_none()
            if not interview:
                result = await db.execute(
                    select(Interview).where(Interview.url.like(f"%/{interview_id}"))
                )
                interview = result.scalar_one_or_none()
                if not interview:
                    raise HTTPException(status_code=404, detail="Interview not found")
        
        return serialize_interview(interview)

@router.post("/update-interview", response_model=InterviewResponse)
async def update_interview(
    interview_id: str = Form(...),
    mode: Optional[str] = Form(None),
    auto_question_generate: Optional[bool] = Form(None),
    manual_questions: Optional[str] = Form(None),
    objective: Optional[str] = Form(None),
    description: Optional[str] = Form(None),
    name: Optional[str] = Form(None),
    difficulty_level: Optional[str] = Form(None),
):
    async with AsyncSessionLocal() as db:
        interview = await get_interview_or_404(db, interview_id)

        original_mode = interview.question_mode
        original_qc = interview.question_count

        if name is not None:
            interview.name = name
        if objective is not None:
            interview.objective = objective
        if description is not None:
            interview.description = description
        if mode is not None:
            interview.question_mode = mode
        if auto_question_generate is not None:
            interview.auto_question_generate = auto_question_generate
        
        if manual_questions is not None:
            parsed_questions = parse_manual_questions(manual_questions)
            
            if interview.question_mode == "dynamic":
                interview.manual_questions = None
            elif interview.auto_question_generate:
                if parsed_questions and len(parsed_questions) > 0:
                    formatted_questions = []
                    for q in parsed_questions:
                        formatted_q = {
                            "id": str(q.get("id")) if q.get("id") else None,
                            "question": q.get("question", ""),
                            "text": q.get("question", ""),
                            "difficulty": q.get("depth_level", "medium")  
                        }
                        if formatted_q["id"]:
                            formatted_questions.append(formatted_q)
                        else:
                            formatted_q["id"] = str(uuid.uuid4())
                            formatted_questions.append(formatted_q)
                    
                    interview.llm_generated_questions = {"questions": formatted_questions}
                    flag_modified(interview, 'llm_generated_questions')
                    interview.manual_questions = None
            else:
                if parsed_questions and len(parsed_questions) > 0:
                    interview.manual_questions = parsed_questions
                else:
                    interview.manual_questions = None
                if auto_question_generate is None:
                    interview.auto_question_generate = False
        
        if difficulty_level is not None:
            if difficulty_level not in ["low", "medium", "high"]:
                difficulty_level = "medium"
            if not interview.context:
                interview.context = {}
            interview.context["difficulty_level"] = difficulty_level
            flag_modified(interview, 'context')  

        if (
            original_mode != interview.question_mode
            or original_qc != interview.question_count
        ):
            interview.llm_generated_questions = None

        await db.commit()
        await db.refresh(interview)
        return serialize_interview(interview)

@router.post("/delete-interview")
async def delete_interview(request: DeleteInterviewRequest):
    async with AsyncSessionLocal() as db:
        interview = await get_interview_or_404(db, request.interview_id)
        try:
            await db.delete(interview)
            await db.commit()
            return {"ok": True, "message": "Interview deleted successfully"}
        except Exception as e:
            await db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to delete interview: {str(e)}")

@router.post("/toggle-interview-status")
async def toggle_interview_status(request: ToggleInterviewStatusRequest):
    async with AsyncSessionLocal() as db:
        interview = await get_interview_or_404(db, request.interview_id)
        interview.is_open = not interview.is_open
        await db.commit()
        await db.refresh(interview)
        return {"ok": True, "is_open": interview.is_open}

@router.post("/list-interview-responses")
async def list_responses(request: ListInterviewResponsesRequest):
    async with AsyncSessionLocal() as db:
        interview = await get_interview_or_404(db, request.interview_id)
        result = await db.execute(select(Response).where(Response.interview_id == request.interview_id))
        rows = result.scalars().all()
        payload = [{
            "response_id": str(r.id),
            "name": r.name,
            "email": r.email,
            "answered_questions": len(r.qa_history or [])
        } for r in rows]
        return {"ok": True, "responses": payload}
