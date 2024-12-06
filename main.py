from typing import List
from fastapi import FastAPI
import asyncpg
import os
from dotenv import load_dotenv
import polars as pl
from pydantic import BaseModel

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI()

db_pool = None

class User(BaseModel):
    id: int = None
    name: str
    email: str

@app.on_event("startup")
async def startup():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    await initialize_db()

@app.on_event("shutdown")
async def shutdown():
    await db_pool.close()

async def initialize_db():
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """)

async def insert_sql(users: List[User]):
    async with db_pool.acquire() as conn:
        for user in users:
            await conn.execute("""
                INSERT INTO users (name, email)
                VALUES ($1, $2)
            """, user.name, user.email)

@app.post("/")
async def add_users(users: List[User]):
    await insert_sql(users)
    return {"message": "Successfully added users"}
    
@app.get("/")
async def get_users():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, email FROM users")

        ids = [row["id"] for row in rows]
        names = [row["name"] for row in rows]
        emails = [row["email"] for row in rows]

        df = pl.DataFrame({"id": ids, "name": names, "email": emails})
        lazy_frame = df.lazy()

        collected_df = lazy_frame.collect()

        return collected_df.to_dicts()
