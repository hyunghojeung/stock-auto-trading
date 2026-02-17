"""Supabase DB 연결"""
from supabase import create_client, Client
from app.core.config import config

def get_db() -> Client:
    return create_client(config.SUPABASE_URL, config.SUPABASE_SERVICE_KEY)

db = get_db()
