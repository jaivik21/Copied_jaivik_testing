# Handles audio chunk stream + session management

import socketio
from utils.redis_utils import create_session, add_audio_chunk, get_audio_chunks, remove_session
from services.stt_service import stt_service 
from sqlalchemy import select
from db import AsyncSessionLocal
from models import Response
import asyncio
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
_sessions = {}

async def _cleanup_session(sid):
    """Helper to gracefully stop tasks and cleanup session resources."""
    sess = _sessions.pop(sid, None)
    if not sess:
        return

    # Signal STT service to stop processing audio
    if "audio_queue" in sess and sess["audio_queue"]:
        await sess["audio_queue"].put(None)

    # Cancel running tasks
    for task_name in ["stt_task", "emitter_task"]:
        if task_name in sess and sess[task_name]:
            try:
                sess[task_name].cancel()
                await sess[task_name]
            except asyncio.CancelledError:
                pass  # This is expected

    # Clean up Redis session if it exists
    if "session_id" in sess:
        await remove_session(sess["session_id"])
        await sio.leave_room(sid, sess["session_id"])

@sio.event
async def connect(sid, environ):
    await sio.emit("connected", {"sid": sid}, to=sid)

@sio.event
async def disconnect(sid):
    await _cleanup_session(sid)

@sio.event
async def start_interview(sid, data):
    interview_id = data.get("interview_id")
    response_id = data.get("response_id")
    
    if not interview_id:
        return {"ok": False, "error": "interview_id is required"}
    
    # Validate that the interview exists
    async with AsyncSessionLocal() as session:
        from models import Interview
        result = await session.execute(select(Interview).where(Interview.id == interview_id))
        interview = result.scalar_one_or_none()
        
        if not interview:
            return {"ok": False, "error": f"Interview with id {interview_id} not found"}
        
        if not interview.is_active:
            return {"ok": False, "error": "Interview is not active"}
    
    # Generate response_id and create session
    # import uuid
    # response_id = str(uuid.uuid4())
    session_id = f"{interview_id}_{response_id}"
    await create_session(session_id)
    
    # CREATE the Response record
    # async with AsyncSessionLocal() as session:
    #     new_response = Response(
    #         id=response_id,
    #         interview_id=interview_id,
    #         name=data.get("name"),
    #         email=data.get("email"),
    #         call_id=data.get("call_id"),
    #         transcripts=[],
    #         is_ended=False,
    #         is_analysed=False,
    #         is_viewed=False
    #     )
    #     session.add(new_response)
    #     await session.commit()
    #     print(f"[DEBUG] Created new Response record: {response_id}")
    
    # _sessions[sid] = {"session_id":session_id, "response_id":response_id}
    # await sio.enter_room(sid, session_id)

    # Added Live STT functions
    audio_queue = asyncio.Queue()
    transcript_queue = asyncio.Queue()

    async def transcript_emitter(sid, t_queue):
        last_partial = ""
        while True:
            try:
                update = await t_queue.get()
                if update is None: break
                text = update["text"] if isinstance(update, dict) else str(update)
                is_final = update.get("is_final") if isinstance(update, dict) else True
                # Optionally: buffer/stitch final results, show partials as they come
                await sio.emit("partial_transcript", {"text": text, "is_final": is_final}, to=sid)
                if is_final:  # On final, you might clear the partial buffer on the frontend
                    last_partial = ""
            except asyncio.CancelledError:
                break
        
    emitter_task = asyncio.create_task(transcript_emitter(sid, transcript_queue))
    stt_task = asyncio.create_task(stt_service.stream_transcribe_session(audio_queue, transcript_queue))
    
    _sessions[sid] = {
        "session_id": session_id, 
        "response_id": response_id,
        "audio_queue": audio_queue,
        "stt_task": stt_task,
        "emitter_task": emitter_task,
    }
    await sio.enter_room(sid, session_id)

    
    return {"ok": True, "session_id": session_id, "response_id":response_id}

@sio.event
async def send_audio_chunk(sid, data):
    #session_id = _sessions.get(sid)
    sess = _sessions.get(sid)
    if not sess:
        return {"ok": False, "error": "No active session"}
    #session_id = sess["session_id"] #updated

    # chunk_bytes = data if isinstance(data, (bytes, bytearray)) else data.get("chunk_data", data)
    # if chunk_bytes :
    #     print(f"[DEBUG] Received audio chunk from {sid}: {len(chunk_bytes)} bytes") 
    # else : 
    #     print(" Empty chunks ")
    # await add_audio_chunk(session_id, chunk_bytes)

    # Updated Send FUnctions
    chunk_bytes = data if isinstance(data, (bytes, bytearray)) else data.get("chunk_data", data)
    
    if chunk_bytes:
        # For live transcription
        await sess["audio_queue"].put(chunk_bytes)
        # For final, high-quality transcription
        await add_audio_chunk(sess["session_id"], chunk_bytes)
        return {"ok": True}
    
    return {"ok": False, "error": "Empty audio chunk"}


@sio.event
async def end_interview(sid, data=None):
    # session_id = _sessions.pop(sid, None)
    # if not session_id:
    #     return {"ok": False, "error": "No active session"}
    
    # #chunks = await get_audio_chunks(session_id)
    # await sio.leave_room(sid, session_id)

    # transcript = await stt_service.transcribe_session(session_id)
    # await remove_session(session_id)
    # await sio.emit("transcript_result",{"text":transcript}, to=sid)
    # print(f"Transcript text : {transcript}")
    
    # return {"ok": True, "transcript": transcript}

    sess = _sessions.get(sid)
    if not sess:
        return {"ok": False, "error": "No active session"}

    session_id = sess["session_id"]
    response_id = sess["response_id"]

    # Stop live streaming first to avoid writing to closing transport
    if "audio_queue" in sess:
        try:
            await sess["audio_queue"].put(None)
        except Exception:
            pass

    for task_name in ["stt_task", "emitter_task"]:
        task = sess.get(task_name)
        if task:
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

    try:
        # Perform a final, high-quality transcription on the complete audio
        final_text = await stt_service.transcribe_session(session_id)
        
        await sio.emit("transcript_result", {"text": final_text}, to=sid)

        # Persist the final transcript to the database
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(Response).where(Response.id == response_id))
            resp = result.scalar_one_or_none()
            if resp:
                # This assumes 'transcripts' is a field on your Response model
                # If not, you may need to add it or store the transcript elsewhere.
                if hasattr(resp, 'transcripts') and isinstance(resp.transcripts, list):
                    resp.transcripts.append(final_text)
                else:
                    # Fallback or create if it doesn't exist.
                    # This depends on your model definition.
                    pass
                resp.is_ended = True
                await session.commit()

        return {"ok": True, "final": True, "transcript": final_text}
    finally:
        # Cleanup Redis session and room
        try:
            await remove_session(session_id)
        except Exception:
            pass
        try:
            await sio.leave_room(sid, session_id)
        except Exception:
            pass
        _sessions.pop(sid, None)
