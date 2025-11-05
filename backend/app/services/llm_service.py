# LLM (OpenAI, Anthropic, etc.)

import json
import re
from typing import Dict, List, Optional, Union
from config_loader import load_config
import openai
from openai import AsyncOpenAI
from openai import AsyncAzureOpenAI
import uuid
from services.summarization_service import summarization_service
from db import AsyncSessionLocal
from models import Interview
from sqlalchemy import select

class LLMService:
    def __init__(self):
        self.config = load_config()  
        self.provider = self.config.get('llm', {}).get('provider', 'openai')
        self.api_key = self.config.get('llm', {}).get('api_key')
        self.model = self.config.get('llm', {}).get('model', 'gpt-4o-mini')
        self.azure_endpoint = self.config.get('llm', {}).get('azure_endpoint')
        self.azure_api_version = self.config.get('llm', {}).get('api_version', '2024-02-01')
        self.azure_deployment = self.config.get('llm', {}).get('deployment', self.model)
        self.max_tokens = self.config.get('llm', {}).get('max_tokens', 1000)
        self.temperature = self.config.get('llm', {}).get('temperature', 0.7)
        
        if self.provider.lower() in ('openai', 'openai_platform'):
            self.client = AsyncOpenAI(api_key=self.api_key)
        elif self.provider.lower() in ('azure', 'azure_openai', 'azure-openai'):
            if AsyncAzureOpenAI is None:
                raise RuntimeError("AsyncAzureOpenAI client not available. Please upgrade the openai package.")
            if not self.api_key:
                raise ValueError("Azure API key is required (llm.api_key).")
            if not self.azure_endpoint:
                raise ValueError("Azure endpoint is required for Azure OpenAI (llm.azure_endpoint in config).")
            if not self.azure_api_version:
                self.azure_api_version = '2024-02-01'
            if not self.azure_deployment:
                raise ValueError("Azure deployment name is required (llm.deployment). Set it to your Azure model deployment name.")
            self.model = self.azure_deployment
            self.client = AsyncAzureOpenAI(
                azure_endpoint=self.azure_endpoint,
                api_key=self.api_key,
                api_version=self.azure_api_version,
            )
    
    def _parse_json(self, text: str):
        text = (text or "").strip()
        try:
            return json.loads(text)
        except Exception:
            pass
        if "```" in text:
            parts = text.split("```")
            candidates = [p for p in parts if p.strip()]
            if len(candidates) >= 1:
                fenced = candidates[0] if len(candidates) == 1 else candidates[1]
                lines = fenced.splitlines()
                if lines and lines[0].strip().lower() in ("json", "javascript", "ts", "typescript", "python"):
                    fenced_json = "\n".join(lines[1:])
                else:
                    fenced_json = fenced
                try:
                    return json.loads(fenced_json)
                except Exception:
                    pass
        match = re.search(r"\{[\s\S]*\}|\[[\s\S]*\]", text)
        if match:
            candidate = match.group(0)
            try:
                return json.loads(candidate)
            except Exception:
                pass
        raise ValueError("LLM response did not contain valid JSON")

    async def generate_questions(self, interview_id: str, context: Dict, question_mode: str = "predefined") -> List[Dict]:
        try:
            context_summary = context.get('context_summary', 'No context available')
            difficulty_level = context.get('difficulty_level', 'medium')
            
            if question_mode == "predefined":
                return await self._generate_predefined_questions(context_summary, context.get("question_count", 5), difficulty_level)
            else:
                return await self._generate_dynamic_question(context_summary, difficulty_level)
                
        except Exception as e:
            print(f"Error generating questions: {str(e)}")
            return []
    
    async def _generate_predefined_questions(self, context_summary: str, question_count: int, difficulty_level: str = "medium", interview_objective: str = "", interview_name: str = "") -> Dict:
        """
        Generate interview questions and description in one call, following Followup AI style.
        Returns a dict with 'questions' (list) and 'description' (str).
        """
        difficulty_distribution = {
            "low": {"easy": 0.7, "medium": 0.3, "hard": 0.0},
            "medium": {"easy": 0.3, "medium": 0.4, "hard": 0.3},
            "high": {"easy": 0.0, "medium": 0.3, "hard": 0.7}
        }
        
        dist = difficulty_distribution.get(difficulty_level, difficulty_distribution["medium"])
        easy_count = max(0, int(question_count * dist["easy"]))
        medium_count = max(0, int(question_count * dist["medium"]))
        hard_count = question_count - easy_count - medium_count
        
        system_prompt = "You are an expert in coming up with follow up questions to uncover deeper insights."
        
        user_prompt = f"""Imagine you are an interviewer specialized in designing interview questions to help hiring managers find candidates with strong technical expertise and project experience, making it easier to identify the ideal fit for the role.

        Interview Title: {interview_name or 'Technical Interview'}
        Interview Objective: {interview_objective or 'Technical skills assessment'}

        Number of questions to be generated: {question_count}

        Follow these detailed guidelines when crafting the questions:
        - Focus on evaluating the candidate's technical knowledge and their experience working on relevant projects. Questions should aim to gauge depth of expertise, problem-solving ability, and hands-on project experience. These aspects carry the most weight.
        - Include questions designed to assess problem-solving skills through practical examples. For instance, how the candidate has tackled challenges in previous projects, and their approach to complex technical issues.
        - Soft skills such as communication, teamwork, and adaptability should be addressed, but given less emphasis compared to technical and problem-solving abilities.
        - Maintain a professional yet approachable tone, ensuring candidates feel comfortable while demonstrating their knowledge.
        - Ask concise and precise open-ended questions that encourage detailed responses. Each question should be 30 words or less for clarity.

        Generate exactly {question_count} interview questions with this distribution:
        - {easy_count} easy questions (basic concepts, fundamental knowledge, entry-level topics)
        - {medium_count} medium questions (intermediate concepts, practical experience, scenario-based)
        - {hard_count} hard questions (advanced concepts, complex problem-solving, deep technical knowledge)

        Use the following context to generate the questions:
        {context_summary}

        Moreover generate a 50 word or less second-person description about the interview to be shown to the user. It should be in the field 'description'. 
        
        CRITICAL REQUIREMENTS FOR DESCRIPTION:
        - Do NOT repeat or paraphrase the objective verbatim
        - Description should be DISTINCT and DIFFERENT from the objective
        - Description should be more conversational and user-friendly (e.g., "In this interview, you'll discuss..." or "This interview focuses on exploring...")
        - If objective is formal/technical, description should be more accessible and clear
        - Description should explain what the candidate will experience, not what the interviewer wants to assess
        - Make it clear to the respondent who's taking the interview what to expect
        - If the objective already explains what will be discussed, the description should add context about format, approach, or what the candidate should prepare

        The field 'questions' should take the format of an array of objects with the following key: question.

        Strictly output only a JSON object with the keys 'questions' and 'description'."""
        
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            max_tokens=1000,
            temperature=0.4
        )
        
        response_text = response.choices[0].message.content
        parsed = self._parse_json(response_text)
        
        if isinstance(parsed, dict):
            questions = parsed.get('questions', [])
            description = parsed.get('description', '')
        else:
            questions = []
            description = ''
        
        if not isinstance(questions, list):
            questions = [questions] if questions else []
        
        normalized_questions = []
        for q in questions[:question_count]:
            if isinstance(q, dict):
                normalized_q = {
                    'id': q.get('id') or str(uuid.uuid4()),
                    'question': q.get('question', ''),
                    'text': q.get('question', q.get('text', '')),
                }
                if 'difficulty' in q:
                    normalized_q['difficulty'] = q['difficulty']
                normalized_questions.append(normalized_q)
            else:
                normalized_questions.append({
                    'id': str(uuid.uuid4()),
                    'question': str(q),
                    'text': str(q)
                })
        
        return {
            'questions': normalized_questions,
            'description': description.strip() if description else ''
        }
    
    async def _generate_dynamic_question(self, context_summary: str, difficulty_level: str = "medium") -> List[Dict]:
        """Generate first question for dynamic mode (only 1 question, difficulty doesn't apply to distribution)"""
        prompt = f"""Generate the first interview question for this role:

        {context_summary}

        The question should:
        - Welcome the candidate warmly
        - Ask about their background/experience
        - Be conversational and professional
        - Be appropriate for the role level

        Return ONLY a JSON object with "question" and "text" fields (both should be the same question text).

        Return only the JSON object, no extra text."""
        
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=200,
            temperature=0.4
        )
        
        question_text = response.choices[0].message.content
        question = self._parse_json(question_text)
        
        if isinstance(question, list):
            question = question[0] if question else {}
        
        question['id'] = str(uuid.uuid4())
        if 'text' not in question:
            question['text'] = question.get('question', '')
        
        return [question]
    
    async def generate_next_dynamic_question(self, interview_id: str, previous_answers: List[Dict]) -> Dict:
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(select(Interview).where(Interview.id == interview_id))
                interview = result.scalar_one_or_none()
                
                if not interview or not interview.context:
                    return {"error": "No context available"}
                
                context_summary = interview.context.get('context_summary', 'No context available')
                
                answers_summary = ""
                for i, ans in enumerate(previous_answers[-3:], 1):
                    q = ans.get('question', 'N/A')
                    a = ans.get('answer', 'N/A')[:200]  
                    answers_summary += f"Q{i}: {q}\nA{i}: {a}...\n\n"
                
                prompt = f"""Generate the next interview question based on this context:

                Job Role: {context_summary}

                Previous Q&A:
                {answers_summary if answers_summary else "This is the first question."}

                Generate a relevant follow-up question that:
                - Builds on the candidate's previous answers
                - Deepens understanding of their experience/skills
                - Is specific to the role
                - Is professional and conversational

                Return ONLY a JSON object with "question" and "text" fields (both contain the same question text).

                Return only the JSON object, no extra text."""
                
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=200,
                    temperature=0.5
                )
                
                question_text = response.choices[0].message.content
                question = self._parse_json(question_text)
                
                if isinstance(question, list):
                    question = question[0] if question else {}
                
                question['id'] = str(uuid.uuid4())
                if 'text' not in question:
                    question['text'] = question.get('question', '')
                
                return question
                
        except Exception as e:
            print(f"Error generating next dynamic question: {str(e)}")
            return {}

    async def analyze_response(self, interview_id: str, transcript: str, question_context: Dict) -> Dict:
        """Analyze a single candidate answer - simplified to reduce tokens"""
        try:
            question = question_context.get('question', '')
            system_prompt = "You are an expert in analyzing interview answers."
            user_prompt = f"""Question: {question}\nAnswer: {transcript}\n\nEvaluate (1-10 scale) and return JSON:\n{{"relevance_score": int, "completeness_score": int, "clarity_score": int, "overall_score": int, "strengths": [str], "weaknesses": [str], "suggestions": [str]}}"""
            
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                max_tokens=300,
                temperature=0.3
            )
            
            return self._parse_json(response.choices[0].message.content)
        except Exception as e:
            print(f"Error analyzing response: {str(e)}")
            raise
    
    async def generate_insights(self, call_summaries: List[str], interview_name: str, interview_objective: str, interview_description: str) -> List[str]:
        """Generate insights from call summaries - Followup AI style"""
        try:
            system_prompt = "You are an expert in uncovering deeper insights from interview question and answer sets."
            
            user_prompt = f"""Generate 3 insights from call summaries highlighting user feedback. Each insight max 25 words. No user names.

            Call Summaries: {chr(10).join(f"- {s}" for s in call_summaries)}

            Interview Title: {interview_name}
            Interview Objective: {interview_objective}
            Interview Description: {interview_description}

            Output JSON: {{"insights": [string, string, string]}}"""
            
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                max_tokens=300,
                temperature=0.5
            )
            
            insights = self._parse_json(response.choices[0].message.content).get("insights", [])
            if not isinstance(insights, list):
                insights = [insights] if insights else []
            
            return [' '.join(str(i).split()[:25]) for i in insights[:3]]
            
        except Exception as e:
            print(f"Error generating insights: {str(e)}")
            return []

    async def generate_final_analysis(self, interview_id: str, qa_history: List[Dict]) -> Dict:
        """Generate final analysis - Followup AI style"""
        try: 
            async with AsyncSessionLocal() as db:
                interview = (await db.execute(select(Interview).where(Interview.id == interview_id))).scalar_one_or_none()
                if not interview:
                    return {"error": "Interview not found"}
                
                # Build transcript
                transcript = "\n".join([
                    f"{'Interviewer' if 'question' in qa else 'Candidate'}: {qa.get('question') or qa.get('answer', '')}"
                    for qa in qa_history
                    if qa.get('question') or qa.get('answer')
                ])
                
                questions_list = interview.llm_generated_questions.get('questions', []) if isinstance(interview.llm_generated_questions, dict) else []
                main_questions = [q.get('question', q.get('text', '')) for q in questions_list if isinstance(q, dict)]
                main_questions_text = "\n".join([f"{i+1}. {q}" for i, q in enumerate(main_questions)])
                
                system_prompt = "You are an expert in analyzing interview transcripts. You must only use the main questions provided and not generate or infer additional questions. You must be strict and accurate in scoring - penalize non-answers, evasive responses, and answers that indicate lack of knowledge."
                
                user_prompt = f"""Analyse the following interview transcript and provide structured feedback:

                Transcript: {transcript}

                Main Interview Questions:
                {main_questions_text}

                IMPORTANT SCORING GUIDELINES:
                - NON-ANSWERS OR EVASIVE RESPONSES (e.g., "no idea", "I don't know", "not sure", "can't answer", "no clue", incomplete responses, very short dismissive answers): These must receive LOW scores.
                  * Overall Score: Should be 0-30 for non-answers or complete lack of knowledge on key questions
                  * Communication Score: Should be 1-3 for non-answers (1=Did not answer, 2=No ability, 3=Great difficulty)
                - If candidate shows no knowledge or gives non-answers to multiple critical questions, overall score MUST reflect this harshly (0-40 range)
                - Communication score should reflect ACTUAL ability to communicate technical content and knowledge, NOT just politeness or fluency of speech
                - A candidate who is polite but says "no idea" should score 1-2 on communication (not 7), as they failed to communicate any useful information
                - Only give scores of 7+ if the candidate actually provides meaningful, relevant answers with some depth

                Based on this transcript and the provided main interview questions, generate the following analytics in JSON format:

                1. Overall Score (0-100) and Overall Feedback (60 words) - consider: Communication Skills, Time Taken to Answer, Confidence, Clarity, Attitude, Relevance, Depth of Knowledge, Problem-Solving, Examples/Evidence, Listening Skills, Consistency, Adaptability.
                   CRITICAL: If candidate gave non-answers or showed lack of knowledge, score must be low (0-40). Only give scores above 60 if candidate demonstrated actual knowledge and provided substantive answers.

                2. Communication Skills: Score (0-10) and Feedback (60 words). Rating: 10=Fully operational, 9=Fully operational with occasional issues, 8=Operational with some issues, 7=Effective despite inaccuracies, 6=Partial command, 5=Basic competence, 4=General meaning only, 3=Great difficulty, 2=No ability, 1=Did not answer.
                   CRITICAL: If candidate said "no idea", "I don't know", gave no substantive answer, or showed inability to communicate knowledge, score MUST be 1-3. Do NOT give 7/10 for politeness when no information was communicated.

                3. Summary for each main question: Output all questions. If not found: "Not Asked". If found but no answer: "Not Answered". Otherwise: cohesive paragraph with candidate's response and any follow-ups.

                4. Soft skills summary (10-15 words): confidence, leadership, adaptability, critical thinking, decision making.

                REMEMBER: Be strict and accurate. A candidate who cannot answer questions or says "no idea" should receive scores that reflect their inability to communicate knowledge, not scores that reward politeness or minimal effort.

                Output JSON:
                {{
                "overallScore": number,
                "overallFeedback": string,
                "communication": {{ "score": number, "feedback": string }},
                "questionSummaries": [{{ "question": string, "summary": string }}],
                "softSkillSummary": string
                }}"""
                                
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    response_format={"type": "json_object"},
                    max_tokens=1500,
                    temperature=0.3
                )
                
                analysis = self._parse_json(response.choices[0].message.content)
                
                result = {
                    "overall_score": analysis.get("overallScore", 0),
                    "overall_feedback": ' '.join(analysis.get("overallFeedback", "").split()[:60]),
                    "communication_score": analysis.get("communication", {}).get("score", 0) if isinstance(analysis.get("communication"), dict) else 0,
                    "communication_feedback": ' '.join((analysis.get("communication", {}).get("feedback", "") if isinstance(analysis.get("communication"), dict) else "").split()[:60]),
                    "question_summaries": analysis.get("questionSummaries", []),
                    "soft_skill_summary": ' '.join(analysis.get("softSkillSummary", "").split()[:15]),
                }
                
                return result
                
        except Exception as e:
            print(f"Error generating final analysis: {str(e)}")
            raise
    

llm_service = LLMService()