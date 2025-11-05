import base64
import io
import json
from pathlib import Path
from typing import Optional
import PyPDF2
import docx
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from models import Interview, Response
from services.tts_service import tts_service

def get_questions_list(interview) -> list:
    if isinstance(interview.llm_generated_questions, list):
        questions = interview.llm_generated_questions or []
        if questions:
            return questions
    if isinstance(interview.llm_generated_questions, dict):
        questions = interview.llm_generated_questions.get("questions", [])
        if questions:
            return questions
    
    if isinstance(interview.manual_questions, list):
        return interview.manual_questions or []
    if isinstance(interview.manual_questions, dict):
        return interview.manual_questions.get("questions", [])
    
    return []

def question_text(q) -> str:
    if isinstance(q, dict):
        return q.get("question") or q.get("text") or ""
    return str(q)

async def synthesize_tts(q_text: str, voice_id: Optional[str] = None) -> Optional[dict]:
    if not q_text:
        return None
    try:
        audio_bytes = await tts_service.synthesize(q_text, voice_id=voice_id)
        return {
            "tts_audio_base64": base64.b64encode(audio_bytes).decode("ascii"),
            "tts_content_type": "audio/mpeg",
        }
    except Exception as e:
        print(f"[WARN] TTS failed: {e}")
        return None

async def get_interview_or_404(db: AsyncSession, interview_id: str) -> Interview:
    result = await db.execute(select(Interview).where(Interview.id == interview_id))
    interview = result.scalar_one_or_none()
    if not interview:
        raise HTTPException(status_code=404, detail="Interview not found")
    return interview

async def get_response_or_404(db: AsyncSession, response_id: str) -> Response:
    result = await db.execute(select(Response).where(Response.id == response_id))
    resp = result.scalar_one_or_none()
    if not resp:
        raise HTTPException(status_code=404, detail="Response not found")
    return resp

def extract_text_from_file(file_content: bytes, filename: str) -> str:
    file_extension = Path(filename).suffix.lower()
    
    if file_extension == '.txt':
        return file_content.decode('utf-8', errors='ignore')
    
    elif file_extension == '.pdf':
        pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text() + "\n"
        return text
    
    elif file_extension in ['.docx', '.doc']:
        doc = docx.Document(io.BytesIO(file_content))
        text = ""
        for paragraph in doc.paragraphs:
            text += paragraph.text + "\n"
        return text
    
    return file_content.decode('utf-8', errors='ignore')

def format_duration(seconds: int) -> str:
    """Format duration in seconds to readable format (e.g., '1m 3s' or '1:03')"""
    if not seconds or seconds <= 0:
        return "0s"
    mins = seconds // 60
    secs = seconds % 60
    if mins > 0:
        return f"{mins}m {secs}s" if secs > 0 else f"{mins}m"
    return f"{secs}s"

def parse_manual_questions(manual_questions: Optional[str]) -> list:
    if not manual_questions:
        return []
    try:
        return json.loads(manual_questions)
    except (json.JSONDecodeError, TypeError):
        return []

async def commit_and_refresh(db: AsyncSession, obj):
    await db.commit()
    await db.refresh(obj)

