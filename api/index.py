"""
IS EASY ChatBot CRM — Backend (FastAPI on Vercel)
Интеграция с Instagram Direct + управление заказами
"""

import os
import json
import hmac
import hashlib
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
import httpx
from supabase import create_client, Client

# ── Supabase ──
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY", "")

def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Meta / Instagram ──
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
META_PAGE_ID = os.getenv("META_PAGE_ID", "")
INSTAGRAM_BUSINESS_ID = os.getenv("INSTAGRAM_BUSINESS_ID", "")
WEBHOOK_VERIFY_TOKEN = os.getenv("WEBHOOK_VERIFY_TOKEN", "iseasy_chatbot_2024")
META_APP_SECRET = os.getenv("META_APP_SECRET", "")

app = FastAPI(title="IS EASY ChatBot CRM", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ═══════════════════════════════════════
#  HEALTH CHECK
# ═══════════════════════════════════════

@app.get("/")
async def root():
    return {"status": "ok", "app": "IS EASY CRM API", "version": "1.0.0"}


@app.get("/api/health")
async def health():
    return {"status": "ok"}


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

    # Проверка подписи от Meta
    if META_APP_SECRET:
        signature = request.headers.get("X-Hub-Signature-256", "")
        expected = "sha256=" + hmac.new(
            META_APP_SECRET.encode(), body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise HTTPException(status_code=403, detail="Invalid signature")

    data = json.loads(body)

    # Обработка входящих сообщений
    for entry in data.get("entry", []):
        for messaging in entry.get("messaging", []):
            sender_id = messaging.get("sender", {}).get("id", "")
            message = messaging.get("message", {})

            if message and sender_id != META_PAGE_ID:
                await process_incoming_message(sender_id, message)

    return {"status": "ok"}


async def process_incoming_message(sender_id: str, message: dict):
    """Обработка входящего сообщения из Instagram."""
    sb = get_supabase()
    message_id = message.get("mid", "")
    text = message.get("text", "")
    attachments = message.get("attachments", [])

    # Определяем тип сообщения
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

    # Получаем или создаём разговор
    conv = sb.table("conversations").select("*").eq(
        "instagram_user_id", sender_id
    ).maybe_single().execute()

    if conv.data:
        conversation_id = conv.data["id"]
        sb.table("conversations").update({
            "last_message_text": text[:200] if text else f"[{msg_type}]",
            "last_message_at": datetime.utcnow().isoformat(),
            "unread_count": conv.data.get("unread_count", 0) + 1,
            "updated_at": datetime.utcnow().isoformat(),
        }).eq("id", conversation_id).execute()
    else:
        profile = await get_instagram_profile(sender_id)
        new_conv = sb.table("conversations").insert({
            "instagram_user_id": sender_id,
            "instagram_username": profile.get("username", ""),
            "client_name": profile.get("name", f"User {sender_id[-4:]}"),
            "avatar_url": profile.get("profile_pic", ""),
            "last_message_text": text[:200] if text else f"[{msg_type}]",
            "last_message_at": datetime.utcnow().isoformat(),
            "unread_count": 1,
        }).execute()
        conversation_id = new_conv.data[0]["id"]

        # Автосоздание клиента
        settings = sb.table("bot_settings").select("value").eq(
            "key", "order_flow"
        ).maybe_single().execute()
        if settings.data and settings.data["value"].get("auto_create_client"):
            sb.table("clients").insert({
                "name": profile.get("name", profile.get("username", "")),
            }).execute()

    # Сохраняем сообщение
    sb.table("messages").insert({
        "conversation_id": conversation_id,
        "instagram_message_id": message_id,
        "direction": "incoming",
        "message_type": msg_type,
        "content": text,
        "media_url": media_url,
    }).execute()

    # Автоответ (если включён)
    await maybe_auto_reply(sb, sender_id, conversation_id)


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


async def maybe_auto_reply(sb: Client, recipient_id: str, conversation_id: int):
    """Отправляет автоответ, если он включён в настройках."""
    settings = sb.table("bot_settings").select("value").eq(
        "key", "auto_reply"
    ).maybe_single().execute()

    if not settings.data:
        return

    config = settings.data["value"]
    if not config.get("enabled"):
        return

    message_text = config.get("message", "Дякуємо за повідомлення!")
    await send_instagram_message(sb, recipient_id, message_text, conversation_id)


async def send_instagram_message(
    sb: Client, recipient_id: str, text: str, conversation_id: Optional[int] = None
):
    """Отправка сообщения в Instagram Direct через Graph API."""
    if not META_ACCESS_TOKEN or not META_PAGE_ID:
        if conversation_id:
            sb.table("messages").insert({
                "conversation_id": conversation_id,
                "direction": "outgoing",
                "message_type": "text",
                "content": text,
            }).execute()
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
        sb.table("messages").insert({
            "conversation_id": conversation_id,
            "instagram_message_id": result.get("message_id", ""),
            "direction": "outgoing",
            "message_type": "text",
            "content": text,
        }).execute()

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
    sb = get_supabase()
    query = sb.table("conversations").select("*").eq("status", status)

    if funnel:
        query = query.eq("funnel", funnel)
    if channel and channel != "all":
        query = query.eq("channel", channel)
    if search:
        query = query.or_(
            f"client_name.ilike.%{search}%,instagram_username.ilike.%{search}%"
        )

    result = query.order("last_message_at", desc=True).range(
        offset, offset + limit - 1
    ).execute()
    return result.data


@app.get("/api/conversations/{conv_id}")
async def get_conversation(conv_id: int):
    """Детали разговора."""
    sb = get_supabase()
    result = sb.table("conversations").select("*").eq(
        "id", conv_id
    ).single().execute()
    return result.data


@app.patch("/api/conversations/{conv_id}")
async def update_conversation(conv_id: int, request: Request):
    """Обновление разговора (теги, заметки, статус, воронка, назначение)."""
    sb = get_supabase()
    body = await request.json()
    body["updated_at"] = datetime.utcnow().isoformat()
    result = sb.table("conversations").update(body).eq(
        "id", conv_id
    ).execute()
    return result.data


@app.post("/api/conversations/{conv_id}/read")
async def mark_conversation_read(conv_id: int):
    """Пометить разговор как прочитанный."""
    sb = get_supabase()
    sb.table("conversations").update({"unread_count": 0}).eq(
        "id", conv_id
    ).execute()
    sb.table("messages").update({"is_read": True}).eq(
        "conversation_id", conv_id
    ).eq("direction", "incoming").eq("is_read", False).execute()
    return {"ok": True}


# ═══════════════════════════════════════
#  API — СООБЩЕНИЯ (MESSAGES)
# ═══════════════════════════════════════

@app.get("/api/conversations/{conv_id}/messages")
async def list_messages(conv_id: int, limit: int = 100, offset: int = 0):
    """Список сообщений в разговоре."""
    sb = get_supabase()
    result = sb.table("messages").select("*").eq(
        "conversation_id", conv_id
    ).order("created_at", desc=False).range(offset, offset + limit - 1).execute()
    return result.data


@app.post("/api/conversations/{conv_id}/messages")
async def send_message(conv_id: int, request: Request):
    """Отправить сообщение клиенту."""
    sb = get_supabase()
    body = await request.json()
    text = body.get("text", "")

    if not text:
        raise HTTPException(status_code=400, detail="Message text is required")

    conv = sb.table("conversations").select("*").eq(
        "id", conv_id
    ).single().execute()

    result = await send_instagram_message(
        sb, conv.data["instagram_user_id"], text, conv_id
    )

    sb.table("conversations").update({
        "last_message_text": text[:200],
        "last_message_at": datetime.utcnow().isoformat(),
        "last_message_dir": "out",
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("id", conv_id).execute()

    return {"ok": True, "result": result}


# ═══════════════════════════════════════
#  API — ЗАКАЗЫ (ORDERS)
# ═══════════════════════════════════════

@app.get("/api/orders")
async def list_orders(status: str = "", limit: int = 50, offset: int = 0):
    """Список заказов."""
    sb = get_supabase()
    query = sb.table("orders").select("*")
    if status:
        query = query.eq("status", status)
    result = query.order("created_at", desc=True).range(
        offset, offset + limit - 1
    ).execute()
    return result.data


@app.post("/api/orders")
async def create_order(request: Request):
    """Создать заказ."""
    sb = get_supabase()
    body = await request.json()
    result = sb.table("orders").insert(body).execute()
    return result.data


@app.patch("/api/orders/{order_id}")
async def update_order(order_id: int, request: Request):
    """Обновить заказ."""
    sb = get_supabase()
    body = await request.json()
    body["updated_at"] = datetime.utcnow().isoformat()
    result = sb.table("orders").update(body).eq("id", order_id).execute()
    return result.data


@app.post("/api/orders/{order_id}/notify")
async def notify_order(order_id: int):
    """Отправить клиенту уведомление о статусе заказа."""
    sb = get_supabase()
    order = sb.table("orders").select("*").eq(
        "id", order_id
    ).single().execute()

    if not order.data:
        raise HTTPException(status_code=404, detail="Order not found")

    conv = sb.table("conversations").select("*").eq(
        "client_name", order.data["client"]
    ).maybe_single().execute()

    if not conv.data:
        return {"ok": False, "error": "Conversation not found"}

    status = order.data.get("status", "")
    ttn = order.data.get("ttn", "")

    messages = {
        "new": "Ваше замовлення оформлено! Очікуйте на підтвердження",
        "production": "Ваше замовлення на виробництві. Скоро буде готове!",
        "shipped": f"Ваше замовлення відправлено! ТТН: {ttn}",
        "at_post": "Ваше замовлення на пошті! Заберіть, будь ласка",
        "received": "Дякуємо за покупку! Будемо раді бачити вас знову!",
    }

    text = messages.get(status, f"Статус замовлення: {status}")
    await send_instagram_message(
        sb, conv.data["instagram_user_id"], text, conv.data["id"]
    )

    return {"ok": True, "message_sent": text}


# ═══════════════════════════════════════
#  API — КАТАЛОГ (PRODUCTS)
# ═══════════════════════════════════════

@app.get("/api/products")
async def list_products():
    """Список товаров."""
    sb = get_supabase()
    result = sb.table("products").select("*").eq(
        "is_active", True
    ).order("created_at", desc=True).execute()
    return result.data


# ═══════════════════════════════════════
#  API — ОПЛАТЫ (PAYMENTS)
# ═══════════════════════════════════════

@app.get("/api/payments")
async def list_payments(matched: str = "", limit: int = 50):
    """Список оплат."""
    sb = get_supabase()
    query = sb.table("payments").select("*")
    if matched == "true":
        query = query.eq("is_matched", True)
    elif matched == "false":
        query = query.eq("is_matched", False)
    result = query.order("payment_date", desc=True).range(0, limit - 1).execute()
    return result.data


@app.patch("/api/payments/{payment_id}")
async def update_payment(payment_id: int, request: Request):
    """Сопоставить оплату с заказом."""
    sb = get_supabase()
    body = await request.json()
    result = sb.table("payments").update(body).eq("id", payment_id).execute()
    return result.data


# ═══════════════════════════════════════
#  API — БЫСТРЫЕ ОТВЕТЫ
# ═══════════════════════════════════════

@app.get("/api/quick-replies")
async def list_quick_replies():
    sb = get_supabase()
    result = sb.table("quick_replies").select("*").order("usage_count", desc=True).execute()
    return result.data


@app.post("/api/quick-replies")
async def create_quick_reply(request: Request):
    sb = get_supabase()
    body = await request.json()
    result = sb.table("quick_replies").insert(body).execute()
    return result.data


# ═══════════════════════════════════════
#  API — НАСТРОЙКИ
# ═══════════════════════════════════════

@app.get("/api/settings")
async def get_settings():
    sb = get_supabase()
    result = sb.table("bot_settings").select("*").execute()
    return {row["key"]: row["value"] for row in result.data}


@app.patch("/api/settings/{key}")
async def update_setting(key: str, request: Request):
    sb = get_supabase()
    body = await request.json()
    result = sb.table("bot_settings").update({
        "value": body,
        "updated_at": datetime.utcnow().isoformat(),
    }).eq("key", key).execute()
    return result.data


# ═══════════════════════════════════════
#  API — КЛИЕНТЫ
# ═══════════════════════════════════════

@app.get("/api/clients")
async def list_clients(search: str = "", limit: int = 50):
    sb = get_supabase()
    query = sb.table("clients").select("*")
    if search:
        query = query.or_(f"name.ilike.%{search}%,phone.ilike.%{search}%")
    result = query.order("created", desc=True).range(0, limit - 1).execute()
    return result.data


@app.post("/api/clients")
async def create_client(request: Request):
    sb = get_supabase()
    body = await request.json()
    result = sb.table("clients").insert(body).execute()
    return result.data


@app.patch("/api/clients/{client_id}")
async def update_client(client_id: int, request: Request):
    sb = get_supabase()
    body = await request.json()
    body["updated_at"] = datetime.utcnow().isoformat()
    result = sb.table("clients").update(body).eq("id", client_id).execute()
    return result.data


# ═══════════════════════════════════════
#  DASHBOARD STATS
# ═══════════════════════════════════════

@app.get("/api/stats")
async def get_stats():
    sb = get_supabase()
    convs = sb.table("conversations").select("id", count="exact").eq(
        "status", "active"
    ).execute()
    unread = sb.table("conversations").select("id", count="exact").gt(
        "unread_count", 0
    ).execute()
    orders_new = sb.table("orders").select("id", count="exact").eq(
        "status", "new"
    ).execute()
    orders_total = sb.table("orders").select("id", count="exact").execute()
    products = sb.table("products").select("id", count="exact").execute()

    return {
        "active_conversations": convs.count or 0,
        "unread_conversations": unread.count or 0,
        "new_orders": orders_new.count or 0,
        "total_orders": orders_total.count or 0,
        "total_products": products.count or 0,
    }
