# Deepgram / Whisper / Azure integration
import os
import aiohttp
from typing import AsyncIterator, Optional, List
from utils.redis_utils import get_audio_chunks
from utils import audio_utils
import asyncio  
import json

class STTProvider:
    async def transcribe(self, audio_bytes: bytes, language: Optional[str] = None) -> str:
        raise NotImplementedError

    # async def stream_transcribe(self, audio_bytes: bytes, language: Optional[str] = None) -> AsyncIterator[str]:
    #     text = await self.transcribe(audio_bytes, language)
    #     yield text
    async def stream_transcribe(self, audio_queue: asyncio.Queue, transcript_queue: asyncio.Queue):
        """Method to handle real-time streaming transcription."""
        raise NotImplementedError


class AzureWhisperProvider(STTProvider):
    def __init__(self):
        self.api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.deployment = os.getenv("AZURE_OPENAI_WHISPER_DEPLOYMENT")
        self.api_version = os.getenv("AZURE_OPENAI_WHISPER_API_VERSION")

        if not all([self.api_key, self.api_base, self.deployment, self.api_version]):
            raise ValueError("Missing Azure Whisper configuration. Check your environment variables.")

    async def transcribe(self, audio_bytes: bytes, language: Optional[str] = None) -> str:
        url = f"{self.api_base}/openai/deployments/{self.deployment}/audio/transcriptions?api-version={self.api_version}"
        headers = {"api-key": self.api_key}

        form = aiohttp.FormData()
        if language:
            form.add_field("language", language)
        form.add_field("file", audio_bytes, filename="audio.wav", content_type="application/octet-stream")

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180)) as session:
            async with session.post(url, headers=headers, data=form) as response:
                data = await response.json()
                if response.status != 200:
                    raise RuntimeError(f"Azure Whisper Error: {response.status} - {data}")
                return data.get("text", "")


class DeepgramProvider(STTProvider):
    def __init__(self):
        self.api_key = os.getenv("DEEPGRAM_API_KEY")
        self.api_url = "https://api.deepgram.com/v1/listen"

        if not self.api_key:
            raise ValueError("Missing Deepgram API key. Please set DEEPGRAM_API_KEY.")

    async def transcribe(self, audio_bytes: bytes, language: Optional[str] = None) -> str:
        headers = {
            "Authorization": f"Token {self.api_key}",
            "Content-Type": "audio/wav"
        }

        params = {}
        if language:
            params["language"] = language

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180)) as session:
            async with session.post(self.api_url, headers=headers, params=params, data=audio_bytes) as response:
                data = await response.json()
                if response.status != 200:
                    raise RuntimeError(f"Deepgram Error: {response.status} - {data}")

                results = data.get("results", {}).get("channels", [{}])[0].get("alternatives", [{}])
                return results[0].get("transcript", "")

    async def stream_transcribe(self, audio_queue: asyncio.Queue, transcript_queue: asyncio.Queue):
        """Connects to Deepgram's streaming API and transcribes audio in real-time."""
        #url = "wss://api.deepgram.com/v1/listen?punctuate=true&interim_results=true&model=nova-2&encoding=linear16&sample_rate=16000"
        url = (
        "wss://api.deepgram.com/v1/listen"
        "?model=nova-2&punctuate=true&interim_results=true&endpointing=50&smart_format=true" #endpointing=silencedetection
         )
        headers = {
            "Authorization": f"Token {self.api_key}",
        }

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, headers=headers) as ws:

                async def sender(ws, audio_queue):
                    """Sends audio chunks from the queue to Deepgram."""
                    while True:
                        chunk = await audio_queue.get()
                        if chunk is None:
                            # Signal Deepgram we're done
                            try:
                                await ws.send_json({'type': 'CloseStream'})
                            except Exception:
                                pass
                            break
                        if not chunk:
                            continue
                        try:
                            await ws.send_bytes(chunk)
                        except ConnectionResetError:
                            break
                        except Exception:
                            break

                async def receiver(ws, transcript_queue):
                    """Receives transcript results from Deepgram and puts them in the queue."""
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if data.get('type') == 'Results':
                                transcript = data.get('channel', {}).get('alternatives', [{}])[0].get('transcript', '')
                                if transcript:
                                    await transcript_queue.put({
                                        "text": transcript,
                                        "is_final": data.get('is_final', False)
                                    })
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break

                sender_task = asyncio.create_task(sender(ws, audio_queue))
                receiver_task = asyncio.create_task(receiver(ws, transcript_queue))
                try:
                    await asyncio.gather(sender_task, receiver_task)
                finally:
                    # Ensure tasks are cancelled on exit
                    for t in (sender_task, receiver_task):
                        if not t.done():
                            t.cancel()


class STTService:
    def __init__(self, provider: Optional[STTProvider] = None):
        self.provider = provider or DeepgramProvider()

    async def transcribe_session(self, session_id: str, language: Optional[str] = None) -> str:
        chunks: List[bytes] = await get_audio_chunks(session_id)
        if not chunks:
            return ""

        audio_data = audio_utils.merge_chunks(chunks)
        audio_data = audio_utils.converted_audio_compatible(audio_data)
        return await self.provider.transcribe(audio_data, language)

    async def stream_transcribe_session(self, audio_queue: asyncio.Queue, transcript_queue: asyncio.Queue):
        # text = await self.transcribe_session(session_id, language)
        # yield text
        """Initiates a streaming transcription session."""
        if hasattr(self.provider, 'stream_transcribe'):
            await self.provider.stream_transcribe(audio_queue, transcript_queue)





stt_service = STTService()
