"""
Simplified Summarization Service
Extracts key details from JD and CV, stores compact context
"""

import json
from typing import Dict, Any
from config_loader import load_config
from openai import AsyncOpenAI
from openai import AsyncAzureOpenAI
import re

class SummarizationService:
    def __init__(self):
        self.config = load_config()  
        self.provider = self.config.get('llm', {}).get('provider', 'openai')
        self.api_key = self.config.get('llm', {}).get('api_key')
        self.model = self.config.get('llm', {}).get('model', 'gpt-4o-mini')
        self.azure_endpoint = self.config.get('llm', {}).get('azure_endpoint')
        self.azure_api_version = self.config.get('llm', {}).get('api_version', '2024-02-01')
        self.azure_deployment = self.config.get('llm', {}).get('deployment', self.model)

        self.client = AsyncAzureOpenAI(
            azure_endpoint=self.azure_endpoint,
            api_key=self.api_key,
            api_version=self.azure_api_version,
        )
    
    async def summarize_jd_cv(self, job_description: str, cv_text: str = None):
        """
        JD-only summarization (CV ignored). Backward-compatible signature.
        """
        try:
            jd_block = f"Job Description:\n{job_description}"
            
            prompt = f"""
            You are an assistant that extracts structured key details from a Job Description.
            
            {jd_block}
            
            Return *only* valid JSON with these exact keys:
            {{
                "summary_text": "One paragraph summary of the role",
                "skills": ["skill1", "skill2", "skill3"],
                "experience_years": 5,
                "role_focus": "backend/frontend/fullstack",
                "keywords": ["keyword1", "keyword2"],
                "education": "Degree information if mentioned",
                "red_flags": ["any concerns if found"]
            }}
            
            Do not include any extra text or explanations.
            """

            response = await self.client.chat.completions.create(
                model=self.azure_deployment,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.3
            )

            summary_text = response.choices[0].message.content.strip()
            match = re.search(r"\{.*\}", summary_text, re.DOTALL)
            if match:
                json_text = match.group(0)
            else:
                raise ValueError(f"Response not valid JSON: {summary_text}")

            summary_data = json.loads(json_text)
            return summary_data

        except Exception as e:
            print(f"Error summarizing JD: {str(e)}")
            return {
                "summary_text": "Software developer position requiring technical skills.",
                "skills": ["Python", "FastAPI", "PostgreSQL"],
                "experience_years": 3,
                "role_focus": "backend",
                "keywords": ["development", "api", "database"],
                "education": "Not specified",
                "red_flags": []
            }
    
    def get_context_for_llm(self, context_data: Dict[str, Any]) -> str:
        """
        Format context data for LLM consumption
        """
        return f"""
        Interview Context:
        
        Role: {context_data.get('role_focus', 'Software Developer')}
        Experience: {context_data.get('experience_years', 3)} years
        Skills: {', '.join(context_data.get('skills', []))}
        Keywords: {', '.join(context_data.get('keywords', []))}
        
        Summary: {context_data.get('summary_text', '')}
        """

    async def summarize_jd(self, jd_text: str):
        return await self.summarize_jd_cv(jd_text, None)

summarization_service = SummarizationService()
