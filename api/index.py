"""
IS EASY ChatBot CRM — Backend (FastAPI on Vercel)
Интеграция с Instagram Direct + управление заказами
Lightweight version: direct httpx REST calls to Supabase (no supabase-py)
"""

import os
import json
import hmac
import hashlib
import traceback
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
import httpx

# ── Supabase config ──
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY", "")
REST_URL = f"{SUPABASE_URL}/rest/v1" if SUPABASE_URL else ""

# ── Meta / Instagram ──
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
META_PAGE_ID = os.getenv("META_PAGE_ID", "")
INSTAGRAM_BUSINESS_ID = os.getenv("INSTAGRAM_BUSINESS_ID", "")
WEBHOOK_VERIFY_TOKEN = os.getenv("WEBHOOK_VERIFY_TOKEN", "iseasy_chatbot_2024")
META_APP_SECRET = os.getenv("META_APP_SECRET", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}


# ── Lightweight Supabase REST helper ──

def _h(**extra) -> dict:
    """Merge base headers with extras."""
    return {**HEADERS, **extra}


async def db_select(
    table: str,
    columns: str = "*",
    filters: dict | None = None,
    order: str = "",
    limit: int | None = None,
    offset: int = 0,
    single: bool = False,
    maybe_single: bool = False,
    count: bool = False,
    or_filter: str = "",
    gt: dict | None = None,
):
    """SELECT from Supabase PostgREST."""
    params = {"select": columns}
    if filters:
        for col, val in filters.items():
            params[col] = f"eq.{val}"
    if gt:
        for col, val in gt.items():
            params[col] = f"gt.{val}"
    if or_filter:
        params["or"] = f"({or_filter})"
    if order:
        params["order"] = order
    if limit:
        params["limit"] = str(limit)
    if offset:
        params["offset"] = str(offset)

    hdrs = dict(HEADERS)
    if single or maybe_single:
        hdrs["Accept"] = "application/json"
    if count:
        hdrs["Prefer"] = "count=exact"
        hdrs["Range-Unit"] = "items"
        hdrs["Range"] = "0-0"

    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{REST_URL}/{table}", params=params, headers=hdrs)

    if count:
        cr = resp.headers.get("content-range", "")
        total = int(cr.split("/")[-1]) if "/" in cr else 0
        return {"data": resp.json(), "count": total}

    data = resp.json()

    if single:
        if isinstance(data, list):
            if len(data) == 0:
                raise HTTPException(status_code=404, detail="Not found")
            return data[0]
        return data

    if maybe_single:
        if isinstance(data, list):
            return data[0] if data else None
        return data

    return data


async def db_insert(table: str, data: dict | list):
    """INSERT into Supabase."""
    hdrs = _h(Prefer="return=representation")
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{REST_URL}/{table}", json=data, headers=hdrs
        )
    result = resp.json()
    return result


async def db_update(table: str, data: dict, filters: dict):
    """UPDATE in Supabase."""
    params = {}
    for col, val in filters.items():
        params[col] = f"eq.{val}"
    hdrs = _h(Prefer="return=representation")
    async with httpx.AsyncClient() as client:
        resp = await client.patch(
            f"{REST_URL}/{table}", json=data, params=params, headers=hdrs
        )
    return resp.json()


# ── FastAPI app ──

app = FastAPI(title="IS EASY ChatBot CRM", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════
#  HEALTH CHECK + DEBUG
# ═══════════════════════════════════════

@app.get("/")
async def root():
    return {"status": "ok", "app": "IS EASY CRM API", "version": "2.0.0"}


@app.get("/api/health")
async def health():
    return {"status": "ok"}


@app.get("/api/debug")
async def debug():
    """Debug endpoint — проверка подключения к Supabase."""
    info = {
        "supabase_url_set": bool(SUPABASE_URL),
        "supabase_url_prefix": SUPABASE_URL[:30] if SUPABASE_URL else "EMPTY",
        "supabase_key_set": bool(SUPABASE_KEY),
        "supabase_key_len": len(SUPABASE_KEY),
        "rest_url": REST_URL[:50] if REST_URL else "EMPTY",
    }
    try:
        data = await db_select("products", columns="id,name", limit=2)
        info["supabase_ok"] = True
        info["products"] = data if isinstance(data, list) else []
    except Exception as e:
        info["supabase_ok"] = False
        info["error"] = str(e)
        info["error_type"] = type(e).__name__
        info["traceback"] = traceback.format_exc()
    return info


# ═══════════════════════════════════════
#  INSTAGRAM WEBHOOK
# ═══════════════════════════════════════

@app.get("/webhook")
async def webhook_verify(
    hub_mode: str = Query(None, alias="hub.mode"),
    hub_verify_token: str = Query(None, alias="hub.verify_token"),
    hub_challenge: str = Query(None, alias="hub.challenge"),
):
    """Верификация вебхука Meta (GET)."""
    if hub_mode == "subscribe" and hub_verify_token == WEBHOOK_VERIFY_TOKEN:
        return PlainTextResponse(hub_challenge)
    raise HTTPException(status_code=403, detail="Verification failed")


@app.post("/webhook")
async def webhook_receive(request: Request):
    """Приём сообщений из Instagram Direct (POST)."""
    body = await request.body()

    if META_APP_SECRET:
        signature = request.headers.get("X-Hub-Signature-256", "")
        expected = "sha256=" + hmac.new(
            META_APP_SECRET.encode(), body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise HTTPException(status_code=403, detail="Invalid signature")

    data = json.loads(body)

    for entry in data.get("entry", []):
        # Direct Messages
        for messaging in entry.get("messaging", []):
            sender_id = messaging.get("sender", {}).get("id", "")
            message = messaging.get("message", {})

            if message and sender_id != META_PAGE_ID:
                await process_incoming_message(sender_id, message)

        # Comments on posts
        for change in entry.get("changes", []):
            if change.get("field") == "comments":
                await process_incoming_comment(change.get("value", {}))

    return {"status": "ok"}


async def process_incoming_message(sender_id: str, message: dict):
    """Обработка входящего сообщения из Instagram."""
    message_id = message.get("mid", "")
    text = message.get("text", "")
    attachments = message.get("attachments", [])

    msg_type = "text"
    media_url = ""
    if attachments:
        att = attachments[0]
        att_type = att.get("type", "")
        if att_type == "image":
            msg_type = "image"
            media_url = att.get("payload", {}).get("url", "")
        elif att_type == "video":
            msg_type = "video"
            media_url = att.get("payload", {}).get("url", "")
        elif att_type == "audio":
            msg_type = "voice"
            media_url = att.get("payload", {}).get("url", "")

    conv = await db_select(
        "conversations",
        filters={"instagram_user_id": sender_id},
        maybe_single=True,
    )

    if conv:
        conversation_id = conv["id"]
        await db_update("conversations", {
            "last_message_text": text[:200] if text else f"[{msg_type}]",
            "last_message_at": datetime.utcnow().isoformat(),
            "unread_count": conv.get("unread_count", 0) + 1,
            "updated_at": datetime.utcnow().isoformat(),
        }, {"id": conversation_id})
    else:
        profile = await get_instagram_profile(sender_id)
        new_conv = await db_insert("conversations", {
            "instagram_user_id": sender_id,
            "instagram_username": profile.get("username", ""),
            "client_name": profile.get("name", f"User {sender_id[-4:]}"),
            "avatar_url": profile.get("profile_pic", ""),
            "last_message_text": text[:200] if text else f"[{msg_type}]",
            "last_message_at": datetime.utcnow().isoformat(),
            "unread_count": 1,
        })
        conversation_id = new_conv[0]["id"]

        settings = await db_select(
            "bot_settings",
            columns="value",
            filters={"key": "order_flow"},
            maybe_single=True,
        )
        if settings and settings["value"].get("auto_create_client"):
            await db_insert("clients", {
                "name": profile.get("name", profile.get("username", "")),
            })

    await db_insert("messages", {
        "conversation_id": conversation_id,
        "instagram_message_id": message_id,
        "direction": "incoming",
        "message_type": msg_type,
        "content": text,
        "media_url": media_url,
    })

    await maybe_auto_reply(sender_id, conversation_id)


async def get_instagram_profile(user_id: str) -> dict:
    """Получение профиля пользователя Instagram через Graph API."""
    if not META_ACCESS_TOKEN:
        return {"username": "", "name": f"User {user_id[-4:]}"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://graph.instagram.com/v21.0/{user_id}",
                params={
                    "fields": "username,name,profile_pic",
                    "access_token": META_ACCESS_TOKEN,
                },
            )
            if resp.status_code == 200:
                return resp.json()
    except Exception:
        pass
    return {"username": "", "name": f"User {user_id[-4:]}"}


async def maybe_auto_reply(recipient_id: str, conversation_id: int):
    """Отправляет автоответ, если он включён в настройках."""
    settings = await db_select(
        "bot_settings",
        columns="value",
        filters={"key": "auto_reply"},
        maybe_single=True,
    )

    if not settings:
        return

    config = settings["value"]
    if not config.get("enabled"):
        return

    message_text = config.get("message", "Дякуємо за повідомлення!")
    await send_instagram_message(recipient_id, message_text, conversation_id)


async def send_instagram_message(
    recipient_id: str, text: str, conversation_id: Optional[int] = None
):
    """Отправка сообщения в Instagram Direct через Graph API."""
    if not META_ACCESS_TOKEN or not META_PAGE_ID:
        if conversation_id:
            await db_insert("messages", {
                "conversation_id": conversation_id,
                "direction": "outgoing",
                "message_type": "text",
                "content": text,
            })
        return {"status": "saved_locally"}

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://graph.instagram.com/v21.0/{META_PAGE_ID}/messages",
            json={
                "recipient": {"id": recipient_id},
                "message": {"text": text},
            },
            headers={"Authorization": f"Bearer {META_ACCESS_TOKEN}"},
        )

    result = resp.json()

    if conversation_id:
        await db_insert("messages", {
            "conversation_id": conversation_id,
            "instagram_message_id": result.get("message_id", ""),
            "direction": "outgoing",
            "message_type": "text",
            "content": text,
        })

    return result


# ═══════════════════════════════════════
#  API — РАЗГОВОРЫ (CONVERSATIONS)
# ═══════════════════════════════════════

@app.get("/api/conversations")
async def list_conversations(
    status: str = "active",
    funnel: str = "",
    channel: str = "",
    search: str = "",
    limit: int = 50,
    offset: int = 0,
):
    """Список разговоров для CRM-панели."""
    filters = {"status": status}
    if funnel:
        filters["funnel"] = funnel
    if channel and channel != "all":
        filters["channel"] = channel

    or_filter = ""
    if search:
        or_filter = f"client_name.ilike.%{search}%,instagram_username.ilike.%{search}%"

    return await db_select(
        "conversations",
        filters=filters,
        or_filter=or_filter,
        order="last_message_at.desc",
        limit=limit,
        offset=offset,
    )


@app.get("/api/conversations/{conv_id}")
async def get_conversation(conv_id: int):
    """Детали разговора."""
    return await db_select("conversations", filters={"id": conv_id}, single=True)


@app.patch("/api/conversations/{conv_id}")
async def update_conversation(conv_id: int, request: Request):
    """Обновление разговора (теги, заметки, статус, воронка, назначение)."""
    body = await request.json()
    body["updated_at"] = datetime.utcnow().isoformat()
    return await db_update("conversations", body, {"id": conv_id})


@app.post("/api/conversations/{conv_id}/read")
async def mark_conversation_read(conv_id: int):
    """Пометить разговор как прочитанный."""
    await db_update("conversations", {"unread_count": 0}, {"id": conv_id})
    params = {
        "conversation_id": f"eq.{conv_id}",
        "direction": "eq.incoming",
        "is_read": "eq.false",
    }
    hdrs = _h(Prefer="return=minimal")
    async with httpx.AsyncClient() as client:
        await client.patch(
            f"{REST_URL}/messages",
            json={"is_read": True},
            params=params,
            headers=hdrs,
        )
    return {"ok": True}


# ═══════════════════════════════════════
#  API — СООБЩЕНИЯ (MESSAGES)
# ═══════════════════════════════════════

@app.get("/api/conversations/{conv_id}/messages")
async def list_messages(conv_id: int, limit: int = 100, offset: int = 0):
    """Список сообщений в разговоре."""
    return await db_select(
        "messages",
        filters={"conversation_id": conv_id},
        order="created_at.asc",
        limit=limit,
        offset=offset,
    )


@app.post("/api/conversations/{conv_id}/messages")
async def send_message(conv_id: int, request: Request):
    """Отправить сообщение клиенту."""
    body = await request.json()
    text = body.get("text", "")

    if not text:
        raise HTTPException(status_code=400, detail="Message text is required")

    conv = await db_select("conversations", filters={"id": conv_id}, single=True)

    result = await send_instagram_message(
        conv["instagram_user_id"], text, conv_id
    )

    await db_update("conversations", {
        "last_message_text": text[:200],
        "last_message_at": datetime.utcnow().isoformat(),
        "last_message_dir": "out",
        "updated_at": datetime.utcnow().isoformat(),
    }, {"id": conv_id})

    return {"ok": True, "result": result}


# ═══════════════════════════════════════
#  API — ЗАКАЗЫ (ORDERS)
# ═══════════════════════════════════════

@app.get("/api/orders")
async def list_orders(status: str = "", limit: int = 50, offset: int = 0):
    """Список заказов."""
    filters = {}
    if status:
        filters["status"] = status
    return await db_select(
        "orders",
        filters=filters,
        order="created_at.desc",
        limit=limit,
        offset=offset,
    )


@app.post("/api/orders")
async def create_order(request: Request):
    """Создать заказ."""
    body = await request.json()
    return await db_insert("orders", body)


@app.patch("/api/orders/{order_id}")
async def update_order(order_id: int, request: Request):
    """Обновить заказ."""
    body = await request.json()
    body["updated_at"] = datetime.utcnow().isoformat()
    return await db_update("orders", body, {"id": order_id})


@app.post("/api/orders/{order_id}/notify")
async def notify_order(order_id: int):
    """Отправить клиенту уведомление о статусе заказа."""
    order = await db_select("orders", filters={"id": order_id}, single=True)

    if not order:
        raise HTTPException(status_code=404, detail="Order not found")

    conv = await db_select(
        "conversations",
        filters={"client_name": order["client"]},
        maybe_single=True,
    )

    if not conv:
        return {"ok": False, "error": "Conversation not found"}

    status = order.get("status", "")
    ttn = order.get("ttn", "")

    messages = {
        "new": "Ваше замовлення оформлено! Очікуйте на підтвердження",
        "production": "Ваше замовлення на виробництві. Скоро буде готове!",
        "shipped": f"Ваше замовлення відправлено! ТТН: {ttn}",
        "at_post": "Ваше замовлення на пошті! Заберіть, будь ласка",
        "received": "Дякуємо за покупку! Будемо раді бачити вас знову!",
    }

    text = messages.get(status, f"Статус замовлення: {status}")
    await send_instagram_message(
        conv["instagram_user_id"], text, conv["id"]
    )

    return {"ok": True, "message_sent": text}


# ═══════════════════════════════════════
#  API — КАТАЛОГ (PRODUCTS)
# ═══════════════════════════════════════

@app.get("/api/products")
async def list_products():
    """Список товаров."""
    return await db_select(
        "products",
        filters={"is_active": True},
        order="created_at.desc",
    )


# ═══════════════════════════════════════
#  API — ОПЛАТЫ (PAYMENTS)
# ═══════════════════════════════════════

@app.get("/api/payments")
async def list_payments(matched: str = "", limit: int = 50):
    """Список оплат."""
    filters = {}
    if matched == "true":
        filters["is_matched"] = True
    elif matched == "false":
        filters["is_matched"] = False
    return await db_select(
        "payments",
        filters=filters,
        order="payment_date.desc",
        limit=limit,
    )


@app.patch("/api/payments/{payment_id}")
async def update_payment(payment_id: int, request: Request):
    """Сопоставить оплату с заказом."""
    body = await request.json()
    return await db_update("payments", body, {"id": payment_id})


# ═══════════════════════════════════════
#  API — БЫСТРЫЕ ОТВЕТЫ
# ═══════════════════════════════════════

@app.get("/api/quick-replies")
async def list_quick_replies():
    return await db_select("quick_replies", order="usage_count.desc")


@app.post("/api/quick-replies")
async def create_quick_reply(request: Request):
    body = await request.json()
    return await db_insert("quick_replies", body)


# ═══════════════════════════════════════
#  API — НАСТРОЙКИ
# ═══════════════════════════════════════

@app.get("/api/settings")
async def get_settings():
    data = await db_select("bot_settings")
    return {row["key"]: row["value"] for row in data}


@app.patch("/api/settings/{key}")
async def update_setting(key: str, request: Request):
    body = await request.json()
    return await db_update("bot_settings", {
        "value": body,
        "updated_at": datetime.utcnow().isoformat(),
    }, {"key": key})


# ═══════════════════════════════════════
#  API — КЛИЕНТЫ
# ═══════════════════════════════════════

@app.get("/api/clients")
async def list_clients(search: str = "", limit: int = 50):
    or_filter = ""
    if search:
        or_filter = f"name.ilike.%{search}%,phone.ilike.%{search}%"
    return await db_select(
        "clients",
        or_filter=or_filter,
        order="created.desc",
        limit=limit,
    )


@app.post("/api/clients")
async def create_client(request: Request):
    body = await request.json()
    return await db_insert("clients", body)


@app.patch("/api/clients/{client_id}")
async def update_client(client_id: int, request: Request):
    body = await request.json()
    body["updated_at"] = datetime.utcnow().isoformat()
    return await db_update("clients", body, {"id": client_id})




# ═══════════════════════════════════════
#  INSTAGRAM COMMENTS
# ═══════════════════════════════════════

async def process_incoming_comment(value: dict):
    comment_id = value.get("id", "")
    media_id = value.get("media", {}).get("id", "")
    from_user = value.get("from", {})
    user_id = from_user.get("id", "")
    username = from_user.get("username", "")
    text = value.get("text", "")
    parent_id = value.get("parent_id", "")

    if user_id == INSTAGRAM_BUSINESS_ID:
        return

    try:
        await db_insert("instagram_comments", {
            "instagram_comment_id": comment_id,
            "instagram_media_id": media_id,
            "instagram_user_id": user_id,
            "username": username,
            "text": text,
            "parent_comment_id": parent_id,
            "is_reply": bool(parent_id),
        })
    except Exception:
        pass


async def reply_to_comment(comment_id: str, text: str):
    if not META_ACCESS_TOKEN:
        return {"status": "no_token"}

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://graph.instagram.com/v21.0/{comment_id}/replies",
            json={"message": text},
            headers={"Authorization": f"Bearer {META_ACCESS_TOKEN}"},
        )

    result = resp.json()

    try:
        await db_insert("instagram_comments", {
            "instagram_comment_id": result.get("id", f"reply_{comment_id}"),
            "instagram_media_id": "",
            "instagram_user_id": INSTAGRAM_BUSINESS_ID,
            "username": "iseasy",
            "text": text,
            "parent_comment_id": comment_id,
            "is_reply": True,
        })
    except Exception:
        pass

    return result


# ═══════════════════════════════════════
#  API — COMMENTS
# ═══════════════════════════════════════

@app.get("/api/comments")
async def list_comments(limit: int = 50, offset: int = 0):
    return await db_select(
        "instagram_comments",
        order="created_at.desc",
        limit=limit,
        offset=offset,
    )


@app.get("/api/comments/by-media/{media_id}")
async def comments_by_media(media_id: str):
    return await db_select(
        "instagram_comments",
        filters={"instagram_media_id": media_id},
        order="created_at.asc",
    )


@app.post("/api/comments/{comment_id}/reply")
async def api_reply_to_comment(comment_id: str, request: Request):
    body = await request.json()
    text = body.get("text", "")
    if not text:
        raise HTTPException(status_code=400, detail="Reply text is required")
    result = await reply_to_comment(comment_id, text)
    return {"ok": True, "result": result}


@app.delete("/api/comments/{comment_id}")
async def delete_comment(comment_id: str):
    if not META_ACCESS_TOKEN:
        raise HTTPException(status_code=400, detail="No access token")
    async with httpx.AsyncClient() as client:
        resp = await client.delete(
            f"https://graph.instagram.com/v21.0/{comment_id}",
            headers={"Authorization": f"Bearer {META_ACCESS_TOKEN}"},
        )
    await db_delete("instagram_comments", {"instagram_comment_id": comment_id})
    return {"ok": True, "result": resp.json()}


# ═══════════════════════════════════════
#  DASHBOARD STATS
# ═══════════════════════════════════════

@app.get("/api/stats")
async def get_stats():
    convs = await db_select("conversations", columns="id", filters={"status": "active"}, count=True)
    unread = await db_select("conversations", columns="id", gt={"unread_count": 0}, count=True)
    orders_new = await db_select("orders", columns="id", filters={"status": "new"}, count=True)
    orders_total = await db_select("orders", columns="id", count=True)
    products = await db_select("products", columns="id", count=True)

    return {
        "active_conversations": convs["count"],
        "unread_conversations": unread["count"],
        "new_orders": orders_new["count"],
        "total_orders": orders_total["count"],
        "total_products": products["count"],
    }
