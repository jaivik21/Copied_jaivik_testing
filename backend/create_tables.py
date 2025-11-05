# DB setup script

import os
from dotenv import load_dotenv
from app.models import Base, Organization, User, Interview, Response, Feedback
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker
import asyncio

load_dotenv()

engine = create_async_engine(os.getenv("DATABASE_URL"), echo=True)

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Tables created successfully!")

asyncio.run(create_tables())
