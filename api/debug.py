import json
import os
import traceback


async def app(scope, receive, send):
            if scope["type"] != "http":
                            return

            info = {
                "scope_path": scope.get("path"),
                "scope_root_path": scope.get("root_path"),
                "scope_type": scope.get("type"),
                "scope_method": scope.get("method"),
            }

    supabase_url = os.getenv("SUPABASE_URL", "")
    supabase_key = os.getenv("SUPABASE_ANON_KEY", "")
    info["supabase_url_set"] = bool(supabase_url)
    info["supabase_url_prefix"] = supabase_url[:30] if supabase_url else "EMPTY"
    info["supabase_key_set"] = bool(supabase_key)
    info["supabase_key_len"] = len(supabase_key)

    try:
                    from supabase import create_client
                    sb = create_client(supabase_url, supabase_key)
                    result = sb.table("products").select("id, name").limit(2).execute()
                    info["supabase_ok"] = True
                    info["products"] = result.data
except Exception as e:
                info["supabase_ok"] = False
                info["error"] = str(e)
                info["error_type"] = type(e).__name__
                info["traceback"] = traceback.format_exc()

    body = json.dumps(info, ensure_ascii=False).encode("utf-8")

    await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [[b"content-type", b"application/json"]],
    })
    await send({
                    "type": "http.response.body",
                    "body": body,
    })
