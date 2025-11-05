# Entry point (Socket.IO + FastAPI app)

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from starlette.middleware.cors import CORSMiddleware
import socketio
from sockets.interview_socket import sio
from utils.redis_utils import close_redis
from socketio import ASGIApp 
import sockets.interview_socket
from routers.interview_router import router as interview_router
from routers.question_router import router as question_router
from routers.response_router import router as response_router
from routers.session_router import router as session_router
from routers.interviewer_router import router as interviewer_router
from db import AsyncSessionLocal, engine, Base
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

sio_app = socketio.ASGIApp(sio)
app.mount("/socket.io", sio_app)

# Include all routers
app.include_router(interview_router)
app.include_router(question_router)
app.include_router(response_router)
app.include_router(session_router)
app.include_router(interviewer_router)

# Serve static files
app.mount("/static", StaticFiles(directory="."), name="static")

@app.get("/")
async def root():
    return {"message": "AI Interview Tool API", "websocket": "/socket.io"}

@app.get("/test")
async def test_frontend():
    """Serve the test frontend"""
    return FileResponse("test_interview.html")

@app.on_event("shutdown")
async def shutdown():
    await close_redis()

@app.get("/api/interview/health")
async def health_check():
    """Health check endpoint"""
    return {"ok": True, "message": "Interview API is healthy"}

@app.post("/api/interview/create-sample")
async def create_sample_interview():
    """Create sample interview data for testing"""
    try:
        from models import Interview, Organization, User
        import uuid
        
        async with AsyncSessionLocal() as db:
            # Create sample organization
            org = Organization(
                id=str(uuid.uuid4()),
                name="Test Organization",
                plan="free"
            )
            
            # Create sample user
            user = User(
                id=str(uuid.uuid4()),
                email="test@example.com",
                organization_id=org.id
            )
            
            # Create sample interview
            interview = Interview(
                id=str(uuid.uuid4()),
                name="Sample Interview",
                description="Test interview for STT",
                organization_id=org.id,
                user_id=user.id
            )
            
            db.add(org)
            db.add(user)
            db.add(interview)
            await db.commit()
            
            return {
                "ok": True,
                "interview_id": str(interview.id),
                "organization_id": str(org.id),
                "user_id": str(user.id)
            }
    except Exception as e:
        return {"ok": False, "error": str(e)}