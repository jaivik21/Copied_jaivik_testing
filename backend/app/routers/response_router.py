from fastapi import APIRouter, Body, HTTPException
from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel
from sqlalchemy import select
from db import AsyncSessionLocal
from models import Response, Interview
from schemas.interview_schema import (
    SubmitAnswerRequest,
    GetOverallAnalysisRequest,
    GetResponseRequest,
    UpdateResponseStatusRequest
)
from services.summarization_service import summarization_service
from services.llm_service import llm_service
from utils.interview_utils import (
    get_interview_or_404,
    get_response_or_404,
    get_questions_list,
    question_text,
    format_duration
)

router = APIRouter(prefix="/api/interview", tags=["responses"])

@router.post("/submit-answer")
async def submit_answer(request: SubmitAnswerRequest):
    async with AsyncSessionLocal() as db:
        response = await get_response_or_404(db, request.response_id)
        interview = await get_interview_or_404(db, str(response.interview_id))

        qa_pair = {
            "question": request.question,
            "answer": request.transcript,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "analysis": {}
        }

        if not response.qa_history:
            response.qa_history = []
        
        updated_qa_history = list(response.qa_history) if response.qa_history else []
        updated_qa_history.append(qa_pair)
        response.qa_history = updated_qa_history
        
        from sqlalchemy.orm.attributes import flag_modified
        flag_modified(response, 'qa_history')
        response.current_question_index += 1

        if interview.context:
            try:
                context_for_llm = summarization_service.get_context_for_llm(interview.context)
                analysis = await llm_service.analyze_response(
                    str(interview.id),
                    request.transcript,
                    {"question": request.question}
                )
                if updated_qa_history:
                    updated_qa_history[-1]["analysis"] = analysis or {}
                    response.qa_history = updated_qa_history
                    flag_modified(response, 'qa_history')
            except Exception as e:
                print(f"[WARN] Analysis failed: {e}")

        
        if interview.question_mode == "dynamic":
            if not interview.question_count or interview.question_count <= 0:
                print(f"[WARN] Dynamic mode interview {interview.id} has no question_count set")
                total_questions = 0
            else:
                total_questions = interview.question_count
        else:
            total_questions = len(get_questions_list(interview))
        
        is_complete = response.current_question_index >= total_questions and total_questions > 0

        if is_complete:
            response.is_completed = True
            
            end_time = datetime.now(timezone.utc)
            if not response.end_time:
                response.end_time = end_time
            
            if response.start_time and response.end_time:
                duration_delta = response.end_time - response.start_time
                duration_seconds = int(duration_delta.total_seconds())
                response.duration = duration_seconds
                print(f"[DEBUG] Interview completed naturally. Duration calculated: {duration_seconds} seconds ({duration_seconds // 60}m {duration_seconds % 60}s)")
            
            if interview.context:
                try:
                    final_analysis = await llm_service.generate_final_analysis(
                        str(interview.id), response.qa_history
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
                    except Exception:
                        pass
                except Exception as e:
                    print(f"[WARN] Final analysis failed: {e}")

        print(f"[DEBUG] Before commit - qa_history length: {len(response.qa_history) if response.qa_history else 0}")
        print(f"[DEBUG] qa_history content: {response.qa_history}")
        print(f"[DEBUG] Response ID: {response.id}")
        
        try:
            await db.flush()
            print(f"[DEBUG] Response in session: {response in db}")
            print(f"[DEBUG] Response is modified: {response in db.dirty if hasattr(db, 'dirty') else 'N/A'}")
            await db.commit()
            print(f"[DEBUG] Commit successful")
            await db.refresh(response)
            print(f"[DEBUG] After commit - qa_history length: {len(response.qa_history) if response.qa_history else 0}")
            print(f"[DEBUG] qa_history content after refresh: {response.qa_history}")
            
        except Exception as e:
            await db.rollback()
            print(f"[ERROR] Failed to commit qa_history: {e}")
            print(f"[ERROR] Exception type: {type(e)}")
            import traceback
            print(f"[ERROR] Traceback: {traceback.format_exc()}")
            raise HTTPException(status_code=500, detail=f"Failed to save response: {str(e)}")

        return {
            "ok": True,
            "complete": is_complete,
            "question_number": response.current_question_index,
            "total_questions": total_questions,
            "questions_answered": len(response.qa_history) if response.qa_history else 0,
            "analysis": qa_pair.get("analysis", {}),
            "final_analysis": getattr(response, "overall_analysis", None) if is_complete else None
        }

@router.post("/get-overall-analysis")
async def get_overall_analysis(request: GetOverallAnalysisRequest):
    async with AsyncSessionLocal() as db:
        interview = await get_interview_or_404(db, request.interview_id)
        result = await db.execute(
            select(Response)
            .where(Response.interview_id == request.interview_id)
            .where(Response.is_completed == True)
        )
        all_completed = result.scalars().all()
        
        responses = [
            r for r in all_completed 
            if (r.qa_history and len(r.qa_history) > 0) or getattr(r, 'overall_analysis', None) is not None
        ]
        
        candidates = []
        total_duration = 0
        sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
        status_counts = {"selected": 0, "potential": 0, "not_selected": 0, "no_status": 0}
        
        for r in responses:
            overall_analysis = getattr(r, "overall_analysis", None)
            if not overall_analysis and r.qa_history:
                try:
                    overall_analysis = await llm_service.generate_final_analysis(
                        str(interview.id), r.qa_history
                    )
                    try:
                        setattr(r, "overall_analysis", overall_analysis)
                        await db.commit()
                    except Exception:
                        pass
                except Exception:
                    pass
            
            overall_score = overall_analysis.get("overall_score", 0) if overall_analysis else 0
            communication_score = overall_analysis.get("communication_score", 0) if overall_analysis else 0
            
            status = r.status if hasattr(r, 'status') and r.status else "no_status"
            if overall_analysis and (not status or status == "no_status"):
                score = overall_score
                if score >= 80:
                    status = "selected"
                elif score >= 60:
                    status = "potential"
                elif score < 40:
                    status = "not_selected"
                else:
                    status = "potential"
                r.status = status
                r.status_source = "auto"
                await db.commit()
            
            if overall_analysis:
                summary = (
                    overall_analysis.get("soft_skill_summary", "") or 
                    overall_analysis.get("call_summary", "") or 
                    overall_analysis.get("overall_feedback", "")
                )
            else:
                summary = ""
            
            candidates.append({
                "response_id": str(r.id),
                "name": r.name,
                "email": r.email,
                "overall_score": overall_score,
                "communication_score": communication_score,
                "summary": summary,
                "status": status,
                "status_source": r.status_source if hasattr(r, 'status_source') else "auto",
                "created_at": r.created_at.isoformat() if r.created_at else None
            })
            
            if r.duration:
                total_duration += r.duration
            
            sentiment = overall_analysis.get("sentiment", "neutral").lower() if overall_analysis else "neutral"
            if sentiment in sentiment_counts:
                sentiment_counts[sentiment] += 1
            else:
                sentiment_counts["neutral"] += 1
            
            if status in status_counts:
                status_counts[status] += 1
            else:
                status_counts["no_status"] += 1
        
        candidates.sort(key=lambda x: x["overall_score"], reverse=True)
        
        total_responses = len(responses)
        avg_duration_sec = int(total_duration / total_responses) if total_responses > 0 else 0
        avg_duration = format_duration(avg_duration_sec)
        completion_rate = 100 if total_responses > 0 else 0
        
        return {
            "ok": True,
            "interview": {
                "id": str(interview.id),
                "name": interview.name,
                "objective": interview.objective or "",
                "description": getattr(interview, "description", None) or ""
            },
            "candidates": candidates,
            "metrics": {
                "average_duration": avg_duration,
                "completion_rate": f"{completion_rate}%",
                "sentiment": sentiment_counts,
                "status": {
                    "total_responses": total_responses,
                    "selected": status_counts["selected"],
                    "potential": status_counts["potential"],
                    "not_selected": status_counts["not_selected"],
                    "no_status": status_counts["no_status"]
                }
            }
        }

@router.post("/get-response")
async def get_response_detail(request: GetResponseRequest):
    async with AsyncSessionLocal() as db:
        response = await get_response_or_404(db, request.response_id)
        interview = await get_interview_or_404(db, str(response.interview_id))
        
        qa_history = response.qa_history or []
        
        overall_analysis = getattr(response, "overall_analysis", None)
        if not overall_analysis and qa_history:
            try:
                overall_analysis = await llm_service.generate_final_analysis(str(interview.id), qa_history)
                try:
                    setattr(response, "overall_analysis", overall_analysis)
                    await db.commit()
                except Exception:
                    pass
            except Exception as e:
                print(f"[WARN] Final analysis failed: {e}")
        
        duration_seconds = response.duration if response.duration else 0
        if response.start_time and response.end_time:
            duration_seconds = int((response.end_time - response.start_time).total_seconds())
        duration_formatted = format_duration(duration_seconds)
        
        all_questions = get_questions_list(interview)
        
        expected_question_count = None
        if interview.question_mode == "dynamic" and interview.question_count:
            expected_question_count = interview.question_count
            while len(all_questions) < expected_question_count:
                all_questions.append({
                    "question": f"Question {len(all_questions) + 1} (not generated - interview ended early)",
                    "text": f"Question {len(all_questions) + 1} (not generated - interview ended early)",
                    "id": None
                })
        elif interview.question_mode == "predefined":
            expected_question_count = len(all_questions)
        
        llm_question_summaries = {}
        if overall_analysis and "question_summaries" in overall_analysis:
            for qs in overall_analysis["question_summaries"]:
                q_text = qs.get("question", "")
                summary_text = qs.get("summary", "")
                llm_question_summaries[q_text] = summary_text
        
        question_summary = []
        for idx, q in enumerate(all_questions):
            q_text = question_text(q) if isinstance(q, dict) else str(q)
            is_asked = idx < len(qa_history)
            
            summary = ""
            if q_text in llm_question_summaries:
                summary = llm_question_summaries[q_text]
            elif is_asked and idx < len(qa_history):
                analysis = qa_history[idx].get("analysis", {})
                if isinstance(analysis, dict):
                    summary = analysis.get("feedback") or analysis.get("summary") or ""
            
            if not summary:
                status = "not_asked"
            elif summary.lower() == "not asked":
                status = "not_asked"
            elif summary.lower() == "not answered":
                status = "not_answered"
            else:
                status = "asked"
            
            question_summary.append({
                "question_number": idx + 1,
                "question": q_text,
                "status": status,
                "summary": summary
            })
        
        transcript = []
        for qa in qa_history:
            question = qa.get("question", "")
            answer = qa.get("answer", "")
            if question:
                transcript.append({"speaker": "AI interviewer", "text": question})
            if answer:
                transcript.append({"speaker": response.name or "Candidate", "text": answer})
        
        overall_score = overall_analysis.get("overall_score", 0) if overall_analysis else 0
        communication_score = overall_analysis.get("communication_score", 0) if overall_analysis else 0
        overall_feedback = overall_analysis.get("feedback") or overall_analysis.get("summary", "") if overall_analysis else ""
        communication_feedback = overall_analysis.get("communication_feedback", "") if overall_analysis else ""
        sentiment = overall_analysis.get("sentiment", "neutral").lower() if overall_analysis else "neutral"
        call_summary = overall_analysis.get("call_summary") or overall_analysis.get("summary", "") if overall_analysis else ""
        
        import json
        debug_data = {
            "interview": {
                "id": str(interview.id),
                "name": interview.name,
                "objective": interview.objective or ""
            },
            "candidate": {
                "response_id": str(response.id),
                "name": response.name,
                "email": response.email,
                "created_at": response.created_at.isoformat() if response.created_at else None
            },
            "recording": {
                "duration": duration_formatted,
                "duration_seconds": duration_seconds,
                "available": duration_seconds > 0
            },
            "general_summary": {
                "overall_score": overall_score,
                "overall_feedback": overall_feedback,
                "communication_score": communication_score,
                "communication_feedback": communication_feedback,
                "sentiment": sentiment,
                "call_summary": call_summary
            },
            "question_summary": question_summary,
            "transcript": transcript,
            "qa_history": qa_history,
            "status": response.status if hasattr(response, 'status') else "no_status",
            "status_source": response.status_source if hasattr(response, 'status_source') else "auto"
        }
        print("Overall Analysis:", json.dumps(debug_data, indent=2, default=str))
        return {
            "ok": True,
            "interview": {
                "id": str(interview.id),
                "name": interview.name,
                "objective": interview.objective or ""
            },
            "candidate": {
                "response_id": str(response.id),
                "name": response.name,
                "email": response.email,
                "created_at": response.created_at.isoformat() if response.created_at else None
            },
            "recording": {
                "duration": duration_formatted,
                "duration_seconds": duration_seconds,
                "available": duration_seconds > 0
            },
            "general_summary": {
                "overall_score": overall_score,
                "overall_feedback": overall_feedback,
                "communication_score": communication_score,
                "communication_feedback": communication_feedback,
                "sentiment": sentiment,
                "call_summary": call_summary
            },
            "question_summary": question_summary,
            "transcript": transcript,
            "qa_history": qa_history,
            "status": response.status if hasattr(response, 'status') else "no_status",
            "status_source": response.status_source if hasattr(response, 'status_source') else "auto"
        }

@router.post("/update-response-status")
async def update_response_status(request: UpdateResponseStatusRequest):
    async with AsyncSessionLocal() as db:
        response = await get_response_or_404(db, request.response_id)
        valid_statuses = ["selected", "shortlisted", "rejected", "not_selected", "potential", "no_status"]
        if request.status not in valid_statuses:
            raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {', '.join(valid_statuses)}")
        
        response.status = request.status
        response.status_source = "manual"
        await db.commit()
        await db.refresh(response)
        
        return {"ok": True, "status": response.status, "status_source": response.status_source}

