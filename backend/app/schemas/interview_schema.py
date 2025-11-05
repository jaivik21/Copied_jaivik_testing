from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid

class UploadCVRequest(BaseModel):
    candidate_name: str
    candidate_email: str


class StartInterviewRequest(BaseModel):
    interview_id: str
    candidate_name: str
    candidate_email: str

class EndInterviewRequest(BaseModel):
    response_id: str
    reason: Optional[str] = "Candidate requested to end interview"

class SubmitAnswerRequest(BaseModel):
    response_id: str
    question: str
    transcript: str


class EndInterviewResponse(BaseModel):
    interview_completed: bool
    total_questions: int
    answered_questions: int
    final_analysis: Optional[Dict[str, Any]] = None
    overall_score: Optional[int] = None
    recommendations: Optional[List[str]] = None
    message: str

class ContextSummaryResponse(BaseModel):
    context_id: str
    summary: Dict[str, Any]

class InterviewResponse(BaseModel):
    id: str
    name: str
    objective: str
    mode: str
    question_count: int
    context: Optional[Dict[str, Any]] = None
    questions: Optional[List[Dict[str, Any]]] = None
    candidate_link: Optional[str] = None
    description: Optional[str] = None
    is_open: Optional[bool] = True

class UpdateInterviewRequest(BaseModel):
    name: Optional[str] = None
    objective: Optional[str] = None
    mode: Optional[str] = None  # 'predefined' | 'dynamic'
    question_count: Optional[int] = None
    auto_question_generate: Optional[bool] = None
    manual_questions: Optional[List[Dict[str, Any]]] = None
    
class DynamicQuestion(BaseModel):
    id: str
    question: str
    text: str

class ResponseAnalysis(BaseModel):
    relevance_score: int
    completeness_score: int
    clarity_score: int
    overall_score: int
    strengths: List[str]
    weaknesses: List[str]
    suggestions: List[str]
    follow_up_questions: List[str]

class GetInterviewRequest(BaseModel):
    interview_id: str

class DeleteInterviewRequest(BaseModel):
    interview_id: str

class ToggleInterviewStatusRequest(BaseModel):
    interview_id: str

class ListInterviewResponsesRequest(BaseModel):
    interview_id: str

class GenerateQuestionsRequest(BaseModel):
    interview_id: str
    question_count: Optional[int] = 5

class GetCurrentQuestionRequest(BaseModel):
    response_id: str
    voice_id: Optional[str] = None

class GetOverallAnalysisRequest(BaseModel):
    interview_id: str

class GetResponseRequest(BaseModel):
    response_id: str

class UpdateResponseStatusRequest(BaseModel):
    response_id: str
    status: str

class GetInterviewerRequest(BaseModel):
    interviewer_id: str

class UpdateInterviewerRequest(BaseModel):
    interviewer_id: str
    name: Optional[str] = None
    persona: Optional[str] = None
    accent: Optional[str] = None
    elevenlabs_voice_id: Optional[str] = None
    avatar_url: Optional[str] = None
    is_active: Optional[bool] = None

class DeleteInterviewerRequest(BaseModel):
    interviewer_id: str