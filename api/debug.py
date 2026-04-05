import os
import traceback
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY", "")

@app.get("/")
async def debug():
    info = {
        "supabase_url_set": bool(SUPABASE_URL),
        "supabase_url_prefix": SUPABASE_URL[:30] if SUPABASE_URL else "EMPTY",
        "supabase_key_set": bool(SUPABASE_KEY),
        "supabase_key_len": len(SUPABASE_KEY),
    }
    try:
        from supabase import create_client, Client
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        result = sb.table("products").select("id, name").limit(2).execute()
        info["supabase_ok"] = True
        info["products"] = result.data
    except Exception as e:
        info["supabase_ok"] = False
        info["error"] = str(e)
        info["error_type"] = type(e).__name__
        info["traceback"] = traceback.format_exc()
    return info
