# Text-to-Speech - Provider pattern (aligned with STT service)

import os
from typing import Optional, AsyncIterator
import aiohttp
from config_loader import load_config


class TTSProvider:
    async def synthesize(self, text: str, voice_id: Optional[str] = None, audio_format: str = "mp3") -> bytes:
        raise NotImplementedError

    async def stream_synthesize(self, text: str, voice_id: Optional[str] = None, audio_format: str = "mp3") -> AsyncIterator[bytes]:
        # Default non-streaming implementation yields a single chunk
        audio = await self.synthesize(text, voice_id, audio_format)
        yield audio


class ElevenLabsProvider(TTSProvider):
    def __init__(self):
        self.config = load_config()
        self.api_key = (
            os.getenv("ELEVENLABS_API_KEY")
            or self.config.get("tts", {}).get("api_key")
        )
        if not self.api_key:
            raise ValueError("Missing ELEVENLABS_API_KEY. Set env or config.yaml tts.api_key")

        self.default_voice_id = (
            os.getenv("ELEVENLABS_VOICE_ID")
            or self.config.get("tts", {}).get("voice_id")
            or "21m00Tcm4TlvDq8ikWAM"
        )

        self.base_url = "https://api.elevenlabs.io/v1"

    async def synthesize(self, text: str, voice_id: Optional[str] = None, audio_format: str = "mp3") -> bytes:
        if not text:
            return b""

        voice_to_use = voice_id or self.default_voice_id
        url = f"{self.base_url}/text-to-speech/{voice_to_use}/stream"

        headers = {
            "xi-api-key": self.api_key,
            "Accept": "audio/mpeg" if audio_format == "mp3" else "audio/wav",
            "Content-Type": "application/json",
        }

        payload = {
            "text": text,
            "voice_settings": {
                "stability": 0.35,
                "similarity_boost": 0.75
            }
        }

        timeout = aiohttp.ClientTimeout(total=180)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                if resp.status != 200:
                    try:
                        err = await resp.json()
                    except Exception:
                        err = await resp.text()
                    raise RuntimeError(f"ElevenLabs TTS error {resp.status}: {err}")
                return await resp.read()


class TTSService:
    def __init__(self, provider: Optional[TTSProvider] = None):
        self.provider = provider or ElevenLabsProvider()

    async def synthesize(self, text: str, voice_id: Optional[str] = None, audio_format: str = "mp3") -> bytes:
        return await self.provider.synthesize(text, voice_id, audio_format)

    async def stream_synthesize(self, text: str, voice_id: Optional[str] = None, audio_format: str = "mp3") -> AsyncIterator[bytes]:
        async for chunk in self.provider.stream_synthesize(text, voice_id, audio_format):
            yield chunk


tts_service = TTSService()