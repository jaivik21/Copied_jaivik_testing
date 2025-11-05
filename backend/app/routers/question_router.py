from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified
from db import AsyncSessionLocal
from models import Interview, Response, Interviewer
from schemas.interview_schema import GenerateQuestionsRequest, GetCurrentQuestionRequest
from services.summarization_service import summarization_service
from services.llm_service import llm_service
from utils.interview_utils import (get_interview_or_404, get_response_or_404,get_questions_list,question_text,synthesize_tts)
import uuid


router = APIRouter(prefix="/api/interview", tags=["questions"])

def normalize_question(q):
    if isinstance(q, dict):
        normalized = {
            "id": q.get('id') or str(uuid.uuid4()),
            "question": q.get('question') or q.get('text') or '',
        }
        if 'text' not in normalized:
            normalized['text'] = normalized['question']
        if 'difficulty' in q:
            normalized['difficulty'] = q['difficulty']
        return normalized
    question_str = str(q)
    return {
        "id": str(uuid.uuid4()),
        "question": question_str,
        "text": question_str
    }

@router.post("/generate-questions")
async def generate_questions(request: GenerateQuestionsRequest):
    async with AsyncSessionLocal() as db:
        interview = await get_interview_or_404(db, request.interview_id)
        
        context_for_llm = summarization_service.get_context_for_llm(interview.context) if interview.context else ""
        target_count = request.question_count if request.question_count and request.question_count > 0 else interview.question_count

        if interview.question_mode == "dynamic" and interview.auto_question_generate:
            generated_description = None
            if not interview.description:
                difficulty_level = (interview.context or {}).get('difficulty_level', 'medium')
                interview_objective = interview.objective or ""
                interview_name = interview.name or ""
                result = await llm_service._generate_predefined_questions(
                    context_for_llm, 1, difficulty_level, interview_objective, interview_name
                )
                generated_description = result.get('description', '')
                if generated_description:
                    interview.description = generated_description
                    flag_modified(interview, 'description')
                    await db.commit()
            
            return {
                "ok": True,
                "questions": [],  
                "mode": interview.question_mode,
                "description": generated_description or interview.description or ""
            }
        
        generated_description = None
        if interview.question_mode == "predefined" and not interview.auto_question_generate and interview.manual_questions:
            manual_list = interview.manual_questions if isinstance(interview.manual_questions, list) else []
            questions = [normalize_question(q) for q in manual_list][:target_count]
            interview.llm_generated_questions = {"questions": questions}
            flag_modified(interview, 'llm_generated_questions')
        elif interview.question_mode == "predefined":
            difficulty_level = (interview.context or {}).get('difficulty_level', 'medium')
            interview_objective = interview.objective or ""
            interview_name = interview.name or ""
            result = await llm_service._generate_predefined_questions(
                context_for_llm, target_count, difficulty_level, interview_objective, interview_name
            )
            questions = result.get('questions', [])
            generated_description = result.get('description', '')
            
            questions = [normalize_question(q) for q in questions[:target_count]]
            interview.llm_generated_questions = {"questions": questions}
            flag_modified(interview, 'llm_generated_questions')
            
            if generated_description and not interview.description:
                interview.description = generated_description
                flag_modified(interview, 'description')
        else:
            questions = []
        
        await db.commit()
        return {
            "ok": True,
            "questions": questions,
            "mode": interview.question_mode,
            "description": generated_description or interview.description or ""
        }

@router.post("/get-current-question")
async def get_current_question(request: GetCurrentQuestionRequest):
    async with AsyncSessionLocal() as db:
        response = await get_response_or_404(db, request.response_id)
        interview = await get_interview_or_404(db, str(response.interview_id))

        questions = get_questions_list(interview)
        
        if interview.question_mode == "dynamic":
            
            max_questions = interview.question_count
            
            if response.current_question_index >= max_questions:
                return {"ok": True, "complete": True, "mode": interview.question_mode, "message": f"Interview complete. You have answered all {max_questions} questions."}
            
            if response.current_question_index < len(questions):
                pass
            else:
                previous_answers = response.qa_history or []
                
                if len(previous_answers) == 0:
                    try:
                        context_for_llm = summarization_service.get_context_for_llm(interview.context) if interview.context else ""
                        difficulty_level = (interview.context or {}).get('difficulty_level', 'medium')
                        generated = await llm_service._generate_dynamic_question(context_for_llm, difficulty_level)
                        
                        if generated:
                            first_q = normalize_question(generated[0] if isinstance(generated, list) else generated)
                            if not interview.llm_generated_questions:
                                interview.llm_generated_questions = {"questions": []}
                            elif not isinstance(interview.llm_generated_questions, dict):
                                interview.llm_generated_questions = {"questions": []}
                            elif "questions" not in interview.llm_generated_questions:
                                interview.llm_generated_questions["questions"] = []
                            
                            existing_questions = interview.llm_generated_questions.get("questions", [])
                            existing_questions.append(first_q)
                            interview.llm_generated_questions["questions"] = existing_questions
                            flag_modified(interview, 'llm_generated_questions')
                            await db.commit()
                            
                            questions = get_questions_list(interview)
                    except Exception as e:
                        print(f"[WARN] Failed to generate first dynamic question: {e}")
                        return {"ok": False, "error": f"Failed to generate question: {str(e)}", "mode": interview.question_mode}
                else:
                    if len(previous_answers) < max_questions:
                        try:
                            next_question = await llm_service.generate_next_dynamic_question(
                                str(interview.id),
                                previous_answers
                            )
                            
                            if next_question and not next_question.get("error"):
                                normalized_q = normalize_question(next_question)
                                if not interview.llm_generated_questions:
                                    interview.llm_generated_questions = {"questions": []}
                                elif not isinstance(interview.llm_generated_questions, dict):
                                    interview.llm_generated_questions = {"questions": []}
                                elif "questions" not in interview.llm_generated_questions:
                                    interview.llm_generated_questions["questions"] = []
                                
                                existing_questions = interview.llm_generated_questions.get("questions", [])
                                existing_questions.append(normalized_q)
                                interview.llm_generated_questions["questions"] = existing_questions
                                from sqlalchemy.orm.attributes import flag_modified
                                flag_modified(interview, 'llm_generated_questions')
                                await db.commit()
                                
                                questions = get_questions_list(interview)
                            else:
                                return {"ok": False, "error": next_question.get("error", "Failed to generate next question"), "mode": interview.question_mode}
                        except Exception as e:
                            print(f"[WARN] Failed to generate next dynamic question: {e}")
                            return {"ok": False, "error": f"Failed to generate question: {str(e)}", "mode": interview.question_mode}
                    else:
                        return {"ok": True, "complete": True, "mode": interview.question_mode}
        
        elif not questions and interview.context and interview.auto_question_generate:
            try:
                context_for_llm = summarization_service.get_context_for_llm(interview.context)
                difficulty_level = (interview.context or {}).get('difficulty_level', 'medium')
                interview_objective = interview.objective or ""
                interview_name = interview.name or ""
                target_count = interview.question_count
                result = await llm_service._generate_predefined_questions(context_for_llm, target_count, difficulty_level, interview_objective, interview_name)
                generated_questions = result.get('questions', []) if isinstance(result, dict) else []
                final_questions = generated_questions[:target_count]
                if len(final_questions) < target_count:
                    print(f"[WARN] LLM generated only {len(final_questions)} questions, expected {target_count}")
                interview.llm_generated_questions = {"questions": final_questions}
                if isinstance(result, dict) and result.get('description') and not interview.description:
                    interview.description = result.get('description')
                    flag_modified(interview, 'description')
                flag_modified(interview, 'llm_generated_questions')
                await db.commit()
                questions = get_questions_list(interview)
            except Exception as e:
                print(f"[WARN] Auto-generate failed: {e}")

        if interview.question_mode == "predefined":
            if len(questions) > 0 and response.current_question_index >= len(questions):
                return {"ok": True, "complete": True, "mode": interview.question_mode}
            if len(questions) == 0:
                return {"ok": False, "error": "No questions available for this interview", "mode": interview.question_mode}
        
    
        if response.current_question_index >= len(questions):
            return {"ok": False, "error": f"Question index {response.current_question_index} out of range. Expected < {len(questions)}", "mode": interview.question_mode}
        
        current_question = questions[response.current_question_index]
        
        if not current_question:
            return {"ok": False, "error": "Question not found at current index", "mode": interview.question_mode}
        q_text = question_text(current_question)
        

        total_questions_display = interview.question_count if interview.question_mode == "dynamic" else len(questions)
        
        result = {
            "ok": True,
            "current_question": current_question,
            "question_number": response.current_question_index + 1,
            "total_questions": total_questions_display,
            "mode": interview.question_mode
        }
        
        voice_id_to_use = request.voice_id
        if not voice_id_to_use and interview.interviewer_id:
            try:
                interviewer_result = await db.execute(
                    select(Interviewer).where(Interviewer.id == interview.interviewer_id)
                )
                interviewer = interviewer_result.scalar_one_or_none()
                if interviewer and interviewer.elevenlabs_voice_id:
                    voice_id_to_use = interviewer.elevenlabs_voice_id
                    print(f"[DEBUG] Using interviewer voice_id: {voice_id_to_use}")
                else:
                    print(f"[DEBUG] Interviewer found but no elevenlabs_voice_id set")
            except Exception as e:
                print(f"[WARN] Failed to fetch interviewer voice_id: {e}")
        
        if not voice_id_to_use:
            try:
                from services.tts_service import tts_service
                if hasattr(tts_service.provider, 'default_voice_id'):
                    voice_id_to_use = tts_service.provider.default_voice_id
                    print(f"[DEBUG] Using default voice_id: {voice_id_to_use}")
            except Exception as e:
                print(f"[WARN] Failed to get default voice_id: {e}")
        
        if voice_id_to_use and q_text:
            print(f"[DEBUG] Generating TTS for question: {q_text[:50]}... with voice_id: {voice_id_to_use}")
            try:
                tts_data = await synthesize_tts(q_text, voice_id_to_use)
                if tts_data:
                    result.update(tts_data)
                    print(f"[DEBUG] TTS audio generated successfully, size: {len(tts_data.get('tts_audio_base64', ''))} chars")
                else:
                    print(f"[WARN] TTS synthesis returned None")
            except Exception as e:
                print(f"[ERROR] TTS synthesis failed: {e}")
                import traceback
                traceback.print_exc()
        else:
            if not voice_id_to_use:
                print(f"[WARN] No voice_id available for TTS (interviewer_id: {interview.interviewer_id})")
            if not q_text:
                print(f"[WARN] No question text available for TTS")
        
        return result

