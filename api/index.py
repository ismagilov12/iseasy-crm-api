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
from datetime import datetime, timedelta
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
    """Приём сообщений из Instagram Direct + комментариев (POST).
    Also detects WayForPay webhook data and routes accordingly."""
    body = await request.body()

    # Try to detect WayForPay data (has merchantAccount or transactionStatus)
    try:
        peek = json.loads(body)
        if isinstance(peek, dict) and ("merchantAccount" in peek or "transactionStatus" in peek or "orderReference" in peek):
            print("[WEBHOOK] Detected WayForPay data, routing to wayforpay handler")
            # Re-create request-like call to wayforpay handler
            return await wayforpay_webhook(request, _body_override=peek)
    except Exception:
        pass

    # Signature verification (temporarily logging only, not blocking)
    if META_APP_SECRET:
        signature = request.headers.get("X-Hub-Signature-256", "")
        if signature:
            expected = "sha256=" + hmac.new(
                META_APP_SECRET.encode(), body, hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(signature, expected):
                print(f"WARNING: Signature mismatch. Got: {signature[:20]}... Expected: {expected[:20]}...")
                # TODO: re-enable after fixing META_APP_SECRET
                # raise HTTPException(status_code=403, detail="Invalid signature")

    data = json.loads(body)

    for entry in data.get("entry", []):
        # Direct Messages
        for messaging in entry.get("messaging", []):
            sender_id = messaging.get("sender", {}).get("id", "")
            recipient_id = messaging.get("recipient", {}).get("id", "")
            message = messaging.get("message", {})
            if not message:
                continue

            # Detect echo (message sent BY the business account)
            is_echo = message.get("is_echo", False)
            is_from_page = sender_id in (META_PAGE_ID, INSTAGRAM_BUSINESS_ID)

            if is_echo or is_from_page:
                # Save outgoing message (sent from Instagram directly, not via CRM)
                await process_echo_message(recipient_id, message)
            else:
                await process_incoming_message(sender_id, message)

        # Comments on posts
        for change in entry.get("changes", []):
            if change.get("field") == "comments":
                await process_incoming_comment(change.get("value", {}))

    return {"status": "ok"}


async def process_echo_message(recipient_id: str, message: dict):
    """Process echo message (sent BY the business, received as webhook echo)."""
    message_id = message.get("mid", "")
    text = message.get("text", "")

    # Find conversation by recipient (the customer)
    conv = await db_select(
        "conversations",
        filters={"instagram_user_id": recipient_id},
        maybe_single=True,
    )
    if not conv:
        return  # No conversation for this recipient

    # Check if this message already exists (avoid duplicates from CRM sends)
    existing = await db_select(
        "messages",
        filters={"instagram_message_id": message_id},
        maybe_single=True,
    )
    if existing:
        # Already saved (e.g. sent from CRM) — make sure direction is outgoing
        if existing.get("direction") != "outgoing":
            await db_update("messages", {"direction": "outgoing"}, {"id": existing["id"]})
        return

    conversation_id = conv["id"]
    await db_insert("messages", {
        "conversation_id": conversation_id,
        "instagram_message_id": message_id,
        "direction": "outgoing",
        "message_type": "text",
        "content": text,
    })

    await db_update("conversations", {
        "last_message_text": text[:200] if text else "[outgoing]",
        "last_message_at": datetime.utcnow().isoformat(),
        "last_message_dir": "out",
        "updated_at": datetime.utcnow().isoformat(),
    }, {"id": conversation_id})


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

    # Determine preview text for conversation list
    reply_to = message.get("reply_to", {})
    is_story_reply = bool(reply_to.get("story"))
    if text:
        preview = ("📷 Відповідь на історію: " + text[:150]) if is_story_reply else text[:200]
    elif is_story_reply:
        preview = "📷 Відповідь на історію"
    else:
        preview = f"[{msg_type}]"

    if conv:
        conversation_id = conv["id"]
        await db_update("conversations", {
            "last_message_text": preview,
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
            "last_message_text": preview,
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

    # Check for story reply (Instagram sends reply_to.story with url & id)
    metadata = None
    reply_to = message.get("reply_to", {})
    story = reply_to.get("story")
    if story:
        metadata = {"reply_to_story": {"url": story.get("url", ""), "id": story.get("id", "")}}
        if not msg_type or msg_type == "text":
            msg_type = "story_reply"

    msg_data = {
        "conversation_id": conversation_id,
        "instagram_message_id": message_id,
        "direction": "incoming",
        "message_type": msg_type,
        "content": text,
        "media_url": media_url,
    }
    if metadata:
        msg_data["metadata"] = json.dumps(metadata)

    await db_insert("messages", msg_data)

    await maybe_agent_reply(sender_id, conversation_id, text)


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
    recipient_id: str, text: str, conversation_id: Optional[int] = None,
    image_url: Optional[str] = None, quick_replies: Optional[list] = None,
):
    """Отправка сообщения в Instagram Direct через Graph API."""
    if not META_ACCESS_TOKEN:
        print(f"SEND: No META_ACCESS_TOKEN, saving locally")
        if conversation_id:
            await db_insert("messages", {
                "conversation_id": conversation_id,
                "direction": "outgoing",
                "message_type": "image" if image_url else "text",
                "content": text,
                "media_url": image_url or "",
            })
        return {"status": "saved_locally"}

    ig_id = INSTAGRAM_BUSINESS_ID or META_PAGE_ID
    url = f"https://graph.instagram.com/v21.0/{ig_id}/messages"
    results = []

    async with httpx.AsyncClient() as client:
        # Send image first if provided
        if image_url:
            print(f"SEND IMAGE: {image_url} to {recipient_id}")
            img_resp = await client.post(
                url,
                json={
                    "recipient": {"id": recipient_id},
                    "message": {
                        "attachment": {
                            "type": "image",
                            "payload": {"url": image_url}
                        }
                    },
                },
                headers={"Authorization": f"Bearer {META_ACCESS_TOKEN}"},
            )
            img_result = img_resp.json()
            print(f"SEND IMAGE RESULT: {img_resp.status_code} {img_result}")
            results.append(img_result)

        # Send text message (with optional quick replies)
        if text:
            msg_payload = {"text": text}
            if quick_replies:
                msg_payload["quick_replies"] = quick_replies
            print(f"SEND TEXT: to {recipient_id}")
            txt_resp = await client.post(
                url,
                json={
                    "recipient": {"id": recipient_id},
                    "message": msg_payload,
                },
                headers={"Authorization": f"Bearer {META_ACCESS_TOKEN}"},
            )
            txt_result = txt_resp.json()
            print(f"SEND TEXT RESULT: {txt_resp.status_code} {txt_result}")
            results.append(txt_result)

    result = results[-1] if results else {}

    if conversation_id:
        await db_insert("messages", {
            "conversation_id": conversation_id,
            "instagram_message_id": result.get("message_id", ""),
            "direction": "outgoing",
            "message_type": "image" if image_url else "text",
            "content": text,
            "media_url": image_url or "",
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
    """Список разговоров для CRM-панели. Включает привязанные заказы."""
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
        columns="*,orders(*)",
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

    # Check if funnel is changing — trigger automations
    old_funnel = None
    new_funnel = body.get("funnel")
    if new_funnel:
        try:
            conv = await db_select("conversations", filters={"id": conv_id}, single=True)
            old_funnel = conv.get("funnel", "")
        except Exception:
            pass

    result = await db_update("conversations", body, {"id": conv_id})

    # Fire automations if funnel changed
    if new_funnel and old_funnel and new_funnel != old_funnel:
        try:
            await run_automations("funnel_change", {
                "conversation_id": conv_id,
                "old_funnel": old_funnel,
                "new_funnel": new_funnel,
            })
        except Exception as e:
            print(f"[AUTOMATIONS] funnel_change error: {e}")

    return result


@app.post("/api/conversations/{conv_id}/read")
async def mark_conversation_read(conv_id: int):
    """Пометить разговор как прочитанный."""
    await db_update("conversations", {"unread_count": 0}, {"id": conv_id})
    # Mark individual messages as read
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
    """Отправить сообщение клиенту (Instagram или Telegram)."""
    body = await request.json()
    text = body.get("text", "")
    image_url = body.get("image_url", "")
    quick_replies_raw = body.get("quick_replies", [])

    if not text and not image_url:
        raise HTTPException(status_code=400, detail="Message text or image_url is required")

    conv = await db_select("conversations", filters={"id": conv_id}, single=True)
    platform = conv.get("platform", "instagram")

    if platform == "telegram" and conv.get("telegram_user_id"):
        # ── Telegram: put message in outbox, worker will send it ──
        outbox_entry = {
            "telegram_user_id": conv["telegram_user_id"],
            "conversation_id": conv_id,
            "text": text,
            "media_url": image_url or "",
            "status": "pending",
        }
        await db_insert("telegram_outbox", outbox_entry)

        # Save message locally so it appears in CRM immediately
        await db_insert("messages", {
            "conversation_id": conv_id,
            "direction": "outgoing",
            "message_type": "image" if image_url else "text",
            "content": text,
            "media_url": image_url or "",
        })

        result = {"status": "queued_telegram"}
    else:
        # ── Instagram: send directly via Graph API ──
        quick_replies = None
        if quick_replies_raw:
            quick_replies = [
                {"content_type": "text", "title": qr.get("title", qr) if isinstance(qr, dict) else str(qr), "payload": qr.get("payload", qr) if isinstance(qr, dict) else str(qr)}
                for qr in quick_replies_raw
            ]

        result = await send_instagram_message(
            conv["instagram_user_id"], text, conv_id,
            image_url=image_url if image_url else None,
            quick_replies=quick_replies,
        )

    await db_update("conversations", {
        "last_message_text": (text or "[Фото]")[:200],
        "last_message_at": datetime.utcnow().isoformat(),
        "last_message_dir": "out",
        "updated_at": datetime.utcnow().isoformat(),
    }, {"id": conv_id})

    return {"ok": True, "result": result}


@app.post("/api/conversations/{conv_id}/messages/image")
async def send_image_message(conv_id: int, request: Request):
    """Send image message to client via Instagram (multipart upload)."""
    from fastapi.responses import JSONResponse
    try:
        form = await request.form()
        image = form.get("image")
        text = form.get("text", "")

        conv = await db_select("conversations", filters={"id": conv_id}, single=True)
        recipient_id = conv["instagram_user_id"]

        # For now, save the message as outgoing with text
        # Instagram image sending requires uploading to a hosting service first
        msg_text = text if text else "[Фото]"

        if META_ACCESS_TOKEN and (INSTAGRAM_BUSINESS_ID or META_PAGE_ID):
            ig_id = INSTAGRAM_BUSINESS_ID or META_PAGE_ID
            # Send text part via Instagram API
            if text:
                url = f"https://graph.instagram.com/v21.0/{ig_id}/messages"
                async with httpx.AsyncClient() as client:
                    resp = await client.post(url, json={
                        "recipient": {"id": recipient_id},
                        "message": {"text": text},
                    }, headers={"Authorization": f"Bearer {META_ACCESS_TOKEN}"})
                    print(f"IMAGE MSG text part: {resp.status_code} {resp.json()}")

        await db_insert("messages", {
            "conversation_id": conv_id,
            "direction": "outgoing",
            "message_type": "image",
            "content": msg_text,
        })

        await db_update("conversations", {
            "last_message_text": msg_text[:200],
            "last_message_at": datetime.utcnow().isoformat(),
            "last_message_dir": "out",
            "updated_at": datetime.utcnow().isoformat(),
        }, {"id": conv_id})

        return {"ok": True}
    except Exception as e:
        print(f"Image upload error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


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


# Маппинг camelCase (frontend) → snake_case (БД)
_ORDER_FIELD_MAP = {
    "type": "order_type",
    "npStatus": "np_status",
    "daysAtPost": "days_at_post",
    "returnTtn": "return_ttn",
    "repairNote": "repair_note",
    "repairPhotos": "repair_photos",
    "paymentMethod": "payment_method",
    "productId": "product_id",
    "clientId": "client_id",
    "conversationId": "conversation_id",
}

# Поля, реально существующие в таблице orders
_ORDER_ALLOWED = {
    "conversation_id", "client_id", "client", "product_id", "product_name",
    "size", "quantity", "price", "total", "paid", "status", "order_type",
    "ttn", "np_status", "days_at_post", "return_ttn", "repair_note",
    "repair_photos", "payment_method", "notes",
}


def _normalize_order_payload(body: dict) -> dict:
    """Приводит поля заказа к snake_case и отбрасывает неизвестные."""
    out = {}
    for k, v in (body or {}).items():
        key = _ORDER_FIELD_MAP.get(k, k)
        if key in _ORDER_ALLOWED:
            out[key] = v
    # size в БД — TEXT
    if "size" in out and out["size"] is not None:
        out["size"] = str(out["size"])
    return out


@app.post("/api/orders")
async def create_order(request: Request):
    """Создать заказ + списать наличие со склада по размеру."""
    raw = await request.json()
    body = _normalize_order_payload(raw)

    # Decrement stock_by_size immediately on order add
    try:
        pid = body.get("product_id")
        size = body.get("size")
        qty = int(body.get("quantity", 1) or 1)
        if pid and size is not None:
            prod = await db_select("products", filters={"id": pid}, maybe_single=True)
            if prod:
                sbs = prod.get("stock_by_size") or {}
                if not isinstance(sbs, dict):
                    sbs = {}
                key = str(size)
                cur = int(sbs.get(key, 0) or 0)
                sbs[key] = max(0, cur - qty)
                total = sum(int(v or 0) for v in sbs.values())
                await db_update(
                    "products",
                    {"stock_by_size": sbs, "stock": total},
                    {"id": pid},
                )
    except Exception as e:
        print("[stock decrement] failed:", e)

    result = await db_insert("orders", body)
    # Вернём единичный объект, а не список — фронту удобнее
    if isinstance(result, list) and len(result) > 0:
        return result[0]
    return result


@app.patch("/api/orders/{order_id}")
async def update_order(order_id: int, request: Request):
    """Обновить заказ. Если изменился status — запускает автоматизации order_status_changed."""
    raw = await request.json()
    body = _normalize_order_payload(raw)
    body["updated_at"] = datetime.utcnow().isoformat()

    # Определяем старый статус, чтобы понять, изменился ли он
    old_status = None
    if "status" in body:
        try:
            prev = await db_select("orders", filters={"id": order_id}, maybe_single=True)
            if prev:
                old_status = prev.get("status")
        except Exception as e:
            print(f"[update_order] failed to fetch old status: {e}")

    result = await db_update("orders", body, {"id": order_id})

    # Если статус реально изменился — фаерим триггер
    try:
        new_status = body.get("status")
        if new_status and new_status != old_status:
            await run_automations("order_status_changed", {
                "order_id": order_id,
                "old_status": old_status or "",
                "new_status": new_status,
                "conversation_id": (prev or {}).get("conversation_id") if old_status is not None else None,
            })
    except Exception as e:
        print(f"[update_order] automation fire failed: {e}")

    return result


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
        "shipped": f"Ваше замовлення відправлено! ТТН: {ttn}\nВідстежити: https://novaposhta.ua/tracking/?cargo_number={ttn}" if ttn else "Ваше замовлення відправлено!",
        "at_post": f"Ваше замовлення на пошті! Заберіть, будь ласка 📦\nТТН: {ttn}" if ttn else "Ваше замовлення на пошті! Заберіть, будь ласка",
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


@app.post("/api/products")
async def create_product(request: Request):
    """Create one product or upsert a list of products."""
    body = await request.json()

    # Accept single product or list
    items = body if isinstance(body, list) else [body]
    results = []

    for item in items:
        # Compute total stock from stock_by_size if provided
        sbs = item.get("stock_by_size") or {}
        total_stock = item.get("stock", 0)
        if isinstance(sbs, dict) and sbs:
            try:
                total_stock = sum(int(v or 0) for v in sbs.values())
            except Exception:
                pass

        row = {
            "name": item.get("name", ""),
            "sku": item.get("sku", ""),
            "category": item.get("category", ""),
            "price": item.get("price", 0),
            "cost": item.get("cost", 0),
            "stock": total_stock,
            "stock_by_size": sbs if isinstance(sbs, (dict, list)) else {},
            "sizes": item.get("sizes", []),
            "gender": item.get("gender", "m"),
            "season": item.get("season", "літо"),
            "description": item.get("description", "") or item.get("sku", ""),
            "image_url": item.get("img", "") or item.get("image_url", ""),
            "photo": item.get("img", "") or item.get("photo", ""),
            "weight": item.get("weight", 0),
            "length": item.get("length", 0),
            "width": item.get("width", 0),
            "height": item.get("height", 0),
            "properties": item.get("properties", []),
            "is_active": True,
        }

        # If product has a numeric id and exists in DB — update it
        prod_id = item.get("id")
        if prod_id and isinstance(prod_id, int) and prod_id < 1e12:
            # Check if exists
            existing = await db_select("products", filters={"id": prod_id}, maybe_single=True)
            if existing:
                updated = await db_update("products", row, {"id": prod_id})
                results.append(updated[0] if isinstance(updated, list) and updated else updated)
                continue

        # Insert new product
        result = await db_insert("products", row)
        results.append(result[0] if isinstance(result, list) and result else result)

    return results


@app.put("/api/products/{product_id}")
async def update_product(product_id: int, request: Request):
    """Update a single product."""
    body = await request.json()
    row = {}
    field_map = {
        "name": "name", "sku": "sku", "category": "category",
        "price": "price", "cost": "cost", "stock": "stock",
        "gender": "gender", "description": "description", "season": "season",
        "weight": "weight", "length": "length", "width": "width", "height": "height",
    }
    for frontend_key, db_key in field_map.items():
        if frontend_key in body:
            row[db_key] = body[frontend_key]
    if "img" in body:
        row["image_url"] = body["img"]
        row["photo"] = body["img"]
    if "image_url" in body:
        row["image_url"] = body["image_url"]
    if "sizes" in body:
        row["sizes"] = body["sizes"] if isinstance(body["sizes"], list) else json.loads(body["sizes"] or "[]")
    if "stock_by_size" in body:
        sbs = body["stock_by_size"] or {}
        row["stock_by_size"] = sbs
        if isinstance(sbs, dict) and sbs:
            try:
                row["stock"] = sum(int(v or 0) for v in sbs.values())
            except Exception:
                pass
    if "properties" in body:
        row["properties"] = body["properties"] or []
    if "is_active" in body:
        row["is_active"] = body["is_active"]

    if not row:
        return {"ok": True}
    return await db_update("products", row, {"id": product_id})


@app.delete("/api/products/{product_id}")
async def delete_product(product_id: int):
    """Soft-delete a product (set is_active=false)."""
    return await db_update("products", {"is_active": False}, {"id": product_id})


# ═══════════════════════════════════════
#  API — PRODUCT PROPERTIES (Global dictionary)
# ═══════════════════════════════════════

@app.get("/api/product-properties")
async def list_product_properties():
    """List all property values from the global dictionary."""
    return await db_select("product_properties", order="type.asc,value.asc")


@app.post("/api/product-properties")
async def create_product_property(request: Request):
    """Add a new property value (or list of them)."""
    body = await request.json()
    items = body if isinstance(body, list) else [body]
    rows = []
    for it in items:
        rows.append({
            "type": (it.get("type") or "").strip(),
            "value": (it.get("value") or "").strip(),
            "label": (it.get("label") or it.get("value") or "").strip(),
        })
    rows = [r for r in rows if r["type"] and r["value"]]
    if not rows:
        return []
    try:
        return await db_insert("product_properties", rows if len(rows) > 1 else rows[0])
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.delete("/api/product-properties/{prop_id}")
async def delete_product_property(prop_id: int):
    """Remove a property value from the global dictionary."""
    return await db_delete("product_properties", {"id": prop_id})


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
    # Try update first
    result = await db_update("bot_settings", {
        "value": body,
        "updated_at": datetime.utcnow().isoformat(),
    }, {"key": key})
    # If no rows updated (empty list), insert new row
    if not result or (isinstance(result, list) and len(result) == 0):
        result = await db_insert("bot_settings", {
            "key": key,
            "value": body,
            "updated_at": datetime.utcnow().isoformat(),
        })
    return result


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
    conversation_id = body.pop("conversation_id", None)
    # Also pop 'branch' → map to 'np_branch' (clients table uses np_branch)
    branch = body.pop("branch", None)
    if branch:
        body["np_branch"] = branch
    result = await db_insert("clients", body)
    # If linked to a conversation, update conversation's client_data and client_id
    if conversation_id and result:
        client = result[0] if isinstance(result, list) else result
        client_id = client.get("id")
        client_data = {
            "id": client_id,
            "surname": body.get("surname", ""),
            "name": body.get("name", ""),
            "phone": body.get("phone", ""),
            "city": body.get("city", ""),
            "branch": branch or body.get("np_branch", ""),
        }
        update_payload = {
            "client_data": json.dumps(client_data),
            "updated_at": datetime.utcnow().isoformat(),
        }
        if client_id:
            update_payload["client_id"] = client_id
        await db_update("conversations", update_payload, {"id": conversation_id})
    return result


@app.patch("/api/clients/{client_id}")
async def update_client(client_id: int, request: Request):
    body = await request.json()
    body["updated_at"] = datetime.utcnow().isoformat()
    return await db_update("clients", body, {"id": client_id})


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


# ═══════════════════════════════════════
#  INSTAGRAM COMMENTS
# ═══════════════════════════════════════

async def process_incoming_comment(value: dict):
    """Process incoming Instagram comment from webhook."""
    try:
        comment_id = value.get("id", "")
        media_id = value.get("media", {}).get("id", "") if isinstance(value.get("media"), dict) else value.get("media_id", "")
        from_user = value.get("from", {})
        user_id = from_user.get("id", "")
        username = from_user.get("username", "")
        text = value.get("text", "")
        parent_id = value.get("parent_id", "")
        timestamp = value.get("created_time", datetime.utcnow().isoformat())

        await db_insert("instagram_comments", {
            "instagram_comment_id": comment_id,
            "instagram_media_id": media_id,
            "instagram_user_id": user_id,
            "username": username,
            "text": text,
            "parent_comment_id": parent_id if parent_id else None,
            "is_reply": bool(parent_id),
            "timestamp": timestamp,
        })
    except Exception as e:
        print(f"Error processing comment: {e}")


async def reply_to_comment(comment_id: str, reply_text: str):
    """Reply to an Instagram comment via Graph API."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"https://graph.instagram.com/v21.0/{comment_id}/replies",
            params={"message": reply_text, "access_token": META_ACCESS_TOKEN},
        )
        return resp.json()


@app.get("/api/comments")
async def list_comments():
    rows = await db_select("instagram_comments", order="created_at.desc", limit=100)
    return rows.get("data", []) if isinstance(rows, dict) else rows


@app.get("/api/media/{media_id}")
async def get_media_info(media_id: str):
    """Fetch Instagram media (post) info — thumbnail, type, permalink."""
    if not META_ACCESS_TOKEN:
        return {"id": media_id, "media_url": "", "media_type": ""}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://graph.instagram.com/v21.0/{media_id}",
                params={
                    "fields": "id,media_type,media_url,thumbnail_url,permalink,caption,timestamp",
                    "access_token": META_ACCESS_TOKEN,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                # For VIDEO, use thumbnail_url as preview
                if data.get("media_type") == "VIDEO" and not data.get("media_url"):
                    data["media_url"] = data.get("thumbnail_url", "")
                return data
            print(f"Media fetch error: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Media fetch exception: {e}")
    return {"id": media_id, "media_url": "", "media_type": ""}


@app.get("/api/comments/by-media/{media_id}")
async def comments_by_media(media_id: str):
    rows = await db_select("instagram_comments", filters={"instagram_media_id": media_id}, order="created_at.desc")
    return rows.get("data", []) if isinstance(rows, dict) else rows


@app.post("/api/comments/{comment_id}/reply")
async def reply_comment(comment_id: str, request: Request):
    body = await request.json()
    text = body.get("text", "")
    if not text:
        raise HTTPException(400, "text is required")

    result = await reply_to_comment(comment_id, text)

    reply_id = result.get("id", "")
    if reply_id:
        await db_insert("instagram_comments", {
            "instagram_comment_id": reply_id,
            "instagram_media_id": "",
            "instagram_user_id": INSTAGRAM_BUSINESS_ID,
            "username": "ультра_взуття",
            "text": text,
            "parent_comment_id": comment_id,
            "is_reply": True,
            "reply_comment_id": comment_id,
            "timestamp": datetime.utcnow().isoformat(),
        })

    return {"status": "ok", "reply_id": reply_id, "api_response": result}


@app.delete("/api/comments/{comment_id}")
async def delete_comment(comment_id: int):
    return await db_delete("instagram_comments", {"id": comment_id})


async def db_delete(table: str, filters: dict) -> dict:
    """DELETE from Supabase table."""
    params = {f"{k}": f"eq.{v}" for k, v in filters.items()}
    async with httpx.AsyncClient() as client:
        resp = await client.delete(
            f"{REST_URL}/{table}", headers=_h(Prefer="return=representation"), params=params
        )
        return resp.json() if resp.status_code < 300 else {"error": resp.text}


# ═══════════════════════════════════════
#  PRIVACY POLICY & DATA DELETION (Meta requirement)
# ═══════════════════════════════════════

from fastapi.responses import HTMLResponse
import base64 as _b64

@app.get("/privacy", response_class=HTMLResponse)
async def privacy_policy():
    return """<!DOCTYPE html>
<html lang="uk"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>IS EASY — Політика конфіденційності</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI',sans-serif;background:#faf9f6;color:#1a1a2e;line-height:1.7;padding:40px 20px}
.c{max-width:720px;margin:0 auto}.logo{font-size:28px;font-weight:800;margin-bottom:8px}h1{font-size:22px;margin:30px 0 12px;color:#1a1a2e}
h2{font-size:17px;margin:24px 0 8px;color:#333}p{margin-bottom:12px;font-size:15px;color:#444}.up{font-size:12px;color:#888;margin-bottom:30px}</style></head>
<body><div class="c">
<div class="logo">IS EASY</div>
<p class="up">Останнє оновлення: квітень 2026</p>
<h1>Політика конфіденційності</h1>
<p>Цей додаток ("IS EASY CRM-бот") використовує дані з Facebook та Instagram для надання послуг підтримки клієнтів бренду IS EASY.</p>
<h2>1. Які дані ми збираємо</h2>
<p>Ми отримуємо та зберігаємо: ідентифікатор користувача Instagram/Facebook, імʼя профілю, текст повідомлень у Direct та коментарів під публікаціями, які надіслані на сторінку IS EASY.</p>
<h2>2. Як ми використовуємо дані</h2>
<p>Дані використовуються виключно для: відповіді на повідомлення та коментарі клієнтів, обробки замовлень, покращення якості обслуговування.</p>
<h2>3. Зберігання даних</h2>
<p>Дані зберігаються на захищених серверах (Supabase, ЄС). Ми не передаємо дані третім особам, окрім випадків, передбачених законом.</p>
<h2>4. Видалення даних</h2>
<p>Ви можете запросити видалення своїх даних, надіславши запит на email: <strong>ulteracompany@gmail.com</strong> або через <a href="/data-deletion">сторінку видалення даних</a>. Ми видалимо ваші дані протягом 30 днів.</p>
<h2>5. Контакти</h2>
<p>Email: ulteracompany@gmail.com<br>Бренд: IS EASY (Україна)</p>
</div></body></html>"""


@app.get("/data-deletion", response_class=HTMLResponse)
async def data_deletion_page():
    return """<!DOCTYPE html>
<html lang="uk"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>IS EASY — Видалення даних</title>
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Segoe UI',sans-serif;background:#faf9f6;color:#1a1a2e;line-height:1.7;padding:40px 20px}
.c{max-width:720px;margin:0 auto}.logo{font-size:28px;font-weight:800;margin-bottom:8px}h1{font-size:22px;margin:30px 0 12px}
p{margin-bottom:12px;font-size:15px;color:#444}ol{margin:12px 0 12px 24px;font-size:15px;color:#444}li{margin-bottom:8px}
.box{background:#fff;border:1px solid #e0ddd6;border-radius:12px;padding:24px;margin:20px 0}</style></head>
<body><div class="c">
<div class="logo">IS EASY</div>
<h1>Видалення даних користувача</h1>
<p>Відповідно до вимог Facebook/Instagram та GDPR, ви маєте право на видалення ваших персональних даних з нашої системи.</p>
<div class="box">
<h2 style="font-size:16px;margin-bottom:12px">Як запросити видалення:</h2>
<ol>
<li>Надішліть email на <strong>ulteracompany@gmail.com</strong> з темою "Видалення даних"</li>
<li>Вкажіть ваш Instagram username або Facebook ID</li>
<li>Ми видалимо всі ваші дані протягом 30 днів та повідомимо вас</li>
</ol>
</div>
<p>Дані, які будуть видалені: повідомлення, коментарі, інформація про замовлення, привʼязані до вашого акаунту.</p>
</div></body></html>"""


@app.post("/data-deletion")
async def data_deletion_callback(request: Request):
    """Facebook Data Deletion callback — handles signed requests from Meta."""
    try:
        body = await request.form()
        signed_request = body.get("signed_request", "")
        if not signed_request or not META_APP_SECRET:
            return {"url": "https://iseasy-crm-api.vercel.app/data-deletion", "confirmation_code": "iseasy_pending"}

        parts = signed_request.split(".", 2)
        if len(parts) != 2:
            return {"url": "https://iseasy-crm-api.vercel.app/data-deletion", "confirmation_code": "iseasy_pending"}

        sig, payload = parts
        # Decode payload
        pad = 4 - len(payload) % 4
        if pad != 4:
            payload += "=" * pad
        data = json.loads(_b64.urlsafe_b64decode(payload))
        user_id = data.get("user_id", "unknown")

        # Generate confirmation code
        code = f"iseasy_del_{user_id}_{int(datetime.utcnow().timestamp())}"

        return {
            "url": f"https://iseasy-crm-api.vercel.app/data-deletion",
            "confirmation_code": code
        }
    except Exception:
        return {"url": "https://iseasy-crm-api.vercel.app/data-deletion", "confirmation_code": "iseasy_error"}


# ═══════════════════════════════════════
#  FUNNEL STAGES (stored in bot_settings)
# ═══════════════════════════════════════

DEFAULT_FUNNELS = [
    {"id": "new", "label": "Нові", "color": "#3b82f6", "icon": "💬"},
    {"id": "processing", "label": "На оформлення", "color": "#f59e0b", "icon": "📋"},
    {"id": "payment", "label": "На оплату", "color": "#8b5cf6", "icon": "💳"},
    {"id": "confirmed", "label": "Оформлено", "color": "#10b981", "icon": "✅"},
    {"id": "shipped", "label": "Відправлено", "color": "#06b6d4", "icon": "📦"},
    {"id": "received", "label": "Отримано", "color": "#059669", "icon": "🎉"},
    {"id": "exchange", "label": "Обмін", "color": "#f97316", "icon": "🔄"},
    {"id": "repair", "label": "Ремонт", "color": "#dc2626", "icon": "🔧"},
    {"id": "closed", "label": "Закрито", "color": "#6b7280", "icon": "✕"},
    {"id": "archived", "label": "Архів", "color": "#9ca3af", "icon": "📁"},
]


@app.get("/api/funnels")
async def get_funnels():
    """Get funnel stages from settings, or return defaults."""
    row = await db_select(
        "bot_settings", columns="value",
        filters={"key": "funnel_stages"}, maybe_single=True,
    )
    if row and isinstance(row, dict):
        val = row.get("value", [])
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except Exception:
                val = DEFAULT_FUNNELS
        if isinstance(val, list) and len(val) > 0:
            return val
    return DEFAULT_FUNNELS


@app.put("/api/funnels")
async def save_funnels(request: Request):
    """Save funnel stages to settings."""
    stages = await request.json()
    if not isinstance(stages, list):
        raise HTTPException(400, "Expected array of funnel stages")

    # Upsert into bot_settings
    existing = await db_select(
        "bot_settings", columns="id",
        filters={"key": "funnel_stages"}, maybe_single=True,
    )
    if existing and isinstance(existing, dict) and existing.get("id"):
        await db_update("bot_settings", {
            "value": json.dumps(stages),
            "updated_at": datetime.utcnow().isoformat(),
        }, {"key": "funnel_stages"})
    else:
        await db_insert("bot_settings", {
            "key": "funnel_stages",
            "value": json.dumps(stages),
            "updated_at": datetime.utcnow().isoformat(),
        })

    return {"status": "ok", "count": len(stages)}


# ═══════════════════════════════════════
#  AUTOMATIONS (CRUD + engine)
# ═══════════════════════════════════════

@app.get("/api/automations")
async def list_automations():
    """Get all automation rules."""
    return await db_select("automations", order="created_at.desc", limit=100)


@app.post("/api/automations")
async def create_automation(request: Request):
    """Create a new automation rule."""
    body = await request.json()
    data = {
        "name": body.get("name", ""),
        "trigger_type": body.get("trigger_type", "funnel_change"),
        "trigger_text": body.get("trigger_text", ""),
        "actions": json.dumps(body.get("actions", [])),
        "is_active": body.get("is_active", True),
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }
    result = await db_insert("automations", data)
    return result[0] if isinstance(result, list) and result else result


@app.patch("/api/automations/{auto_id}")
async def update_automation(auto_id: int, request: Request):
    """Update an automation rule."""
    body = await request.json()
    update_data = {"updated_at": datetime.utcnow().isoformat()}
    for field in ["name", "trigger_type", "trigger_text", "is_active"]:
        if field in body:
            update_data[field] = body[field]
    if "actions" in body:
        update_data["actions"] = json.dumps(body["actions"]) if isinstance(body["actions"], list) else body["actions"]
    result = await db_update("automations", update_data, {"id": auto_id})
    return result[0] if isinstance(result, list) and result else {"status": "ok"}


@app.delete("/api/automations/{auto_id}")
async def delete_automation(auto_id: int):
    """Delete an automation rule."""
    async with httpx.AsyncClient() as client:
        resp = await client.delete(
            f"{REST_URL}/automations?id=eq.{auto_id}",
            headers=_h(),
        )
    return {"status": "ok"} if resp.status_code in (200, 204) else {"status": "error"}


# ═══════════════════════════════════════
#  API — КОМАНДА (TEAM MEMBERS)
# ═══════════════════════════════════════

_TEAM_ALLOWED = {"name", "role", "telegram_user_id", "is_active"}


@app.get("/api/team")
async def list_team():
    """Список усіх учасників команди."""
    return await db_select("team_members", order="created_at.desc", limit=200)


@app.post("/api/team")
async def create_team_member(request: Request):
    body = await request.json()
    data = {k: v for k, v in (body or {}).items() if k in _TEAM_ALLOWED}
    if not data.get("name"):
        raise HTTPException(status_code=400, detail="name is required")
    result = await db_insert("team_members", data)
    if isinstance(result, list) and result:
        return result[0]
    return result


@app.patch("/api/team/{member_id}")
async def update_team_member(member_id: int, request: Request):
    body = await request.json()
    data = {k: v for k, v in (body or {}).items() if k in _TEAM_ALLOWED}
    if not data:
        return {"status": "noop"}
    result = await db_update("team_members", data, {"id": member_id})
    return result[0] if isinstance(result, list) and result else {"status": "ok"}


@app.delete("/api/team/{member_id}")
async def delete_team_member(member_id: int):
    async with httpx.AsyncClient() as client:
        resp = await client.delete(
            f"{REST_URL}/team_members?id=eq.{member_id}",
            headers=_h(),
        )
    return {"status": "ok"} if resp.status_code in (200, 204) else {"status": "error"}


# ═══════════════════════════════════════
#  API — ЗАДАЧІ (TASKS)
# ═══════════════════════════════════════

_TASK_ALLOWED = {"title", "description", "assignee_id", "order_id", "status", "deadline", "source"}


@app.get("/api/tasks")
async def list_tasks(status: str = "", limit: int = 200):
    filters = {}
    if status:
        filters["status"] = status
    return await db_select("tasks", filters=filters, order="created_at.desc", limit=limit)


@app.post("/api/tasks")
async def create_task(request: Request):
    body = await request.json()
    data = {k: v for k, v in (body or {}).items() if k in _TASK_ALLOWED}
    if not data.get("title"):
        raise HTTPException(status_code=400, detail="title is required")
    data.setdefault("status", "open")
    data.setdefault("source", "manual")
    result = await db_insert("tasks", data)
    if isinstance(result, list) and result:
        return result[0]
    return result


@app.patch("/api/tasks/{task_id}")
async def update_task(task_id: int, request: Request):
    body = await request.json()
    data = {k: v for k, v in (body or {}).items() if k in _TASK_ALLOWED}
    data["updated_at"] = datetime.utcnow().isoformat()
    result = await db_update("tasks", data, {"id": task_id})
    return result[0] if isinstance(result, list) and result else {"status": "ok"}


@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: int):
    async with httpx.AsyncClient() as client:
        resp = await client.delete(
            f"{REST_URL}/tasks?id=eq.{task_id}",
            headers=_h(),
        )
    return {"status": "ok"} if resp.status_code in (200, 204) else {"status": "error"}


async def run_automations(trigger_type: str, context: dict):
    """
    Execute matching automations.
    trigger_type: 'funnel_change', 'new_message', 'payment_received', 'order_created', 'order_status_changed'
    context: {conversation_id, old_funnel, new_funnel, message_text, order_id, old_status, new_status, ...}
    """
    try:
        autos = await db_select(
            "automations",
            filters={"trigger_type": trigger_type, "is_active": True},
        )
        if not isinstance(autos, list):
            return

        for auto in autos:
            trigger_text = auto.get("trigger_text", "")
            actions = auto.get("actions", [])
            if isinstance(actions, str):
                try:
                    actions = json.loads(actions)
                except Exception:
                    actions = []

            should_run = False

            if trigger_type == "funnel_change":
                # trigger_text = target funnel id, e.g. "processing"
                if trigger_text == context.get("new_funnel", ""):
                    should_run = True
            elif trigger_type == "new_message":
                # trigger_text = keyword to match
                msg = context.get("message_text", "").lower()
                if trigger_text.lower() in msg:
                    should_run = True
            elif trigger_type == "order_status_changed":
                # trigger_text = target order status id, e.g. "shipped"
                if trigger_text and trigger_text == context.get("new_status", ""):
                    should_run = True
            elif trigger_type in ("payment_received", "order_created"):
                should_run = True  # Always fire for these triggers

            if not should_run:
                continue

            # Execute actions
            conv_id = context.get("conversation_id")
            for action in actions:
                atype = action.get("type", "")
                if atype == "send_message" and conv_id:
                    msg_text = action.get("text", "")
                    if msg_text:
                        await _send_message_to_conversation(conv_id, msg_text)
                elif atype == "change_funnel" and conv_id:
                    new_f = action.get("funnel", "")
                    if new_f:
                        await db_update("conversations", {"funnel": new_f}, {"id": conv_id})
                elif atype == "add_tag" and conv_id:
                    tag = action.get("tag", "")
                    if tag:
                        # Append tag to conversation tags array
                        conv = await db_select("conversations", filters={"id": conv_id}, single=True)
                        tags = conv.get("tags", []) or []
                        if tag not in tags:
                            tags.append(tag)
                            await db_update("conversations", {"tags": tags}, {"id": conv_id})
                elif atype == "create_task":
                    # action = {type:"create_task", assignee_id, title, notify_telegram?}
                    try:
                        assignee_id = action.get("assignee_id")
                        title = (action.get("title") or "").strip()
                        if not title:
                            # Автозаголовок, если не указан
                            if trigger_type == "order_status_changed":
                                title = f"Замовлення #{context.get('order_id','')}: статус → {context.get('new_status','')}"
                            else:
                                title = f"Автозадача ({trigger_type})"
                        task_row = {
                            "title": title[:500],
                            "description": action.get("description", "") or "",
                            "assignee_id": int(assignee_id) if assignee_id else None,
                            "order_id": context.get("order_id"),
                            "status": "open",
                            "source": "automation",
                        }
                        await db_insert("tasks", task_row)

                        # Уведомляем сотрудника в Telegram (если задан telegram_user_id)
                        if assignee_id:
                            member = await db_select(
                                "team_members",
                                filters={"id": int(assignee_id)},
                                maybe_single=True,
                            )
                            tg_id = (member or {}).get("telegram_user_id") or ""
                            if tg_id:
                                notify_text = f"📋 Нова задача\n{title}"
                                if context.get("order_id"):
                                    notify_text += f"\nЗамовлення #{context.get('order_id')}"
                                try:
                                    await db_insert("telegram_outbox", {
                                        "telegram_user_id": str(tg_id),
                                        "conversation_id": None,
                                        "text": notify_text,
                                        "media_url": "",
                                        "status": "pending",
                                    })
                                except Exception as e:
                                    print(f"[create_task] telegram notify failed: {e}")
                    except Exception as e:
                        print(f"[create_task] action failed: {e}")

            # Increment run count
            await db_update("automations", {
                "run_count": (auto.get("run_count", 0) or 0) + 1,
                "updated_at": datetime.utcnow().isoformat(),
            }, {"id": auto["id"]})

    except Exception as e:
        print(f"[AUTOMATIONS] Error: {e}")


async def _send_message_to_conversation(conv_id: int, text: str):
    """Send a message in a conversation (outgoing)."""
    conv = await db_select("conversations", filters={"id": conv_id}, single=True)
    ig_user_id = conv.get("instagram_user_id", "")

    # Save to DB
    await db_insert("messages", {
        "conversation_id": conv_id,
        "direction": "outgoing",
        "content": text,
        "message_type": "text",
        "created_at": datetime.utcnow().isoformat(),
    })

    # Send via Instagram API if possible
    if ig_user_id and META_ACCESS_TOKEN:
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"https://graph.instagram.com/v21.0/me/messages",
                    json={
                        "recipient": {"id": ig_user_id},
                        "message": {"text": text},
                    },
                    params={"access_token": META_ACCESS_TOKEN},
                )
        except Exception as e:
            print(f"[AUTO MSG] Instagram send error: {e}")


# ═══════════════════════════════════════
#  PAYMENT WEBHOOKS — WayForPay & Monobank
# ═══════════════════════════════════════

import re
from difflib import SequenceMatcher


def _normalize_name(name: str) -> str:
    """Normalize Ukrainian/Russian name for fuzzy matching."""
    if not name:
        return ""
    return re.sub(r"[^а-яіїєґa-z\s]", "", name.lower().strip())


def _fuzzy_match(a: str, b: str) -> float:
    """Compare two strings with SequenceMatcher, return similarity 0..1."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, _normalize_name(a), _normalize_name(b)).ratio()


async def _auto_match_payment(payment_id: int, payer_name: str, amount: float):
    """
    Try to auto-match a payment to an order by payer surname.
    Logic:
      1. Get all orders with status in ('new','processing','payment') where paid < total
      2. For each order, get the client (via client_id or conversation.client_data)
      3. Fuzzy-match payer_name vs client surname
      4. If match >= 0.7, link payment to order
    """
    try:
        # Get unmatched orders (not fully paid)
        orders = await db_select(
            "orders",
            columns="id,conversation_id,client_id,client,total,paid,status",
            or_filter="status.eq.new,status.eq.processing,status.eq.payment",
            order="created_at.desc",
            limit=100,
        )
        if not orders or (isinstance(orders, dict) and "error" in orders):
            return None

        best_match = None
        best_score = 0.0

        for order in orders:
            order_paid = float(order.get("paid") or 0)
            order_total = float(order.get("total") or 0)
            if order_paid >= order_total:
                continue

            # Try to get client name from the order's client field or client_id
            client_name = order.get("client", "")

            # If no name on order, try conversation's client_data
            if not client_name and order.get("conversation_id"):
                conv = await db_select(
                    "conversations",
                    columns="client_data,client_name",
                    filters={"id": order["conversation_id"]},
                    maybe_single=True,
                )
                if conv:
                    cd = conv.get("client_data")
                    if isinstance(cd, str):
                        try:
                            cd = json.loads(cd)
                        except Exception:
                            cd = {}
                    if isinstance(cd, dict):
                        client_name = cd.get("surname", "") + " " + cd.get("name", "")
                    if not client_name.strip():
                        client_name = conv.get("client_name", "")

            # If still no name, try clients table
            if not client_name.strip() and order.get("client_id"):
                cl = await db_select(
                    "clients",
                    columns="surname,name",
                    filters={"id": order["client_id"]},
                    maybe_single=True,
                )
                if cl:
                    client_name = (cl.get("surname", "") + " " + cl.get("name", "")).strip()

            if not client_name.strip():
                continue

            score = _fuzzy_match(payer_name, client_name)
            if score > best_score:
                best_score = score
                best_match = order

        # Match threshold
        if best_match and best_score >= 0.65:
            new_paid = float(best_match.get("paid") or 0) + amount
            order_total = float(best_match.get("total") or 0)
            new_status = "confirmed" if new_paid >= order_total else best_match.get("status", "payment")

            # Update payment
            await db_update("payments", {
                "matched_order_id": best_match["id"],
                "matched_conversation_id": best_match.get("conversation_id"),
                "is_matched": True,
            }, {"id": payment_id})

            # Update order paid amount
            await db_update("orders", {
                "paid": new_paid,
                "status": new_status,
                "updated_at": datetime.utcnow().isoformat(),
            }, {"id": best_match["id"]})

            return {
                "matched": True,
                "order_id": best_match["id"],
                "score": round(best_score, 2),
                "new_paid": new_paid,
                "fully_paid": new_paid >= order_total,
            }

        return {"matched": False, "best_score": round(best_score, 2) if best_score > 0 else 0}

    except Exception as e:
        print(f"Auto-match error: {e}")
        traceback.print_exc()
        return {"matched": False, "error": str(e)}


# ── WayForPay Webhook ──

@app.post("/webhook/wayforpay")
async def wayforpay_webhook(request: Request, _body_override: dict = None):
    """
    WayForPay sends POST with JSON:
    {
      "merchantAccount": "...",
      "orderReference": "...",
      "amount": 1400,
      "currency": "UAH",
      "authCode": "...",
      "transactionStatus": "Approved",
      "reasonCode": 1100,
      "clientName": "Бондар Олена",
      "email": "...",
      "phone": "...",
      ...
    }
    We must respond with:
    {
      "orderReference": "...",
      "status": "accept",
      "time": <unix_timestamp>,
      "signature": "<hmac_md5>"
    }
    """
    if _body_override is not None:
        body = _body_override
    else:
        try:
            body = await request.json()
        except Exception:
            body = {}

    tx_status = body.get("transactionStatus", "")
    order_ref = body.get("orderReference", "")
    amount_raw = body.get("amount", 0)
    amount = float(amount_raw) if amount_raw else 0
    payer_name = body.get("clientName", "") or body.get("client_name", "")
    currency = body.get("currency", "UAH")
    auth_code = body.get("authCode", "")

    # Extract exact payment time from WFP data
    wfp_created = body.get("createdDate", "") or body.get("processingDate", "")
    if wfp_created and isinstance(wfp_created, (int, float)):
        # Unix timestamp
        payment_dt = datetime.utcfromtimestamp(wfp_created).isoformat()
    elif wfp_created and isinstance(wfp_created, str) and wfp_created.isdigit():
        payment_dt = datetime.utcfromtimestamp(int(wfp_created)).isoformat()
    else:
        payment_dt = datetime.utcnow().isoformat()

    print(f"[WFP] Webhook received: status={tx_status}, ref={order_ref}, amount={amount}, payer={payer_name}, time={payment_dt}")

    # Only process successful payments
    pay_status = "success" if tx_status == "Approved" else tx_status.lower()

    # Save payment to DB
    payment_data = {
        "source": "wayforpay",
        "external_id": order_ref,
        "payer_name": payer_name,
        "amount": amount,
        "currency": currency,
        "status": pay_status,
        "raw_data": json.dumps(body),
        "payment_date": payment_dt,
    }
    result = await db_insert("payments", payment_data)
    payment_id = None
    if isinstance(result, list) and result:
        payment_id = result[0].get("id")
    elif isinstance(result, dict):
        payment_id = result.get("id")

    # Auto-match if successful
    match_result = None
    if pay_status == "success" and payment_id and payer_name:
        match_result = await _auto_match_payment(payment_id, payer_name, amount)

    # Build WFP response with signature
    # signature = HMAC_MD5(merchantSecretKey, merchantAccount;orderReference;time;status)
    now_ts = int(datetime.utcnow().timestamp())
    response_status = "accept"

    # Try to get merchant secret from settings
    wfp_settings = await db_select(
        "bot_settings", columns="value",
        filters={"key": "wayforpay"}, maybe_single=True,
    )
    merchant_secret = ""
    if wfp_settings and isinstance(wfp_settings, dict):
        val = wfp_settings.get("value", {})
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except Exception:
                val = {}
        merchant_secret = val.get("merchant_secret", "")

    merchant_account = body.get("merchantAccount", "")
    sign_string = f"{merchant_account};{order_ref};{now_ts};{response_status}"
    signature = ""
    if merchant_secret:
        signature = hmac.new(
            merchant_secret.encode("utf-8"),
            sign_string.encode("utf-8"),
            hashlib.md5,
        ).hexdigest()

    return {
        "orderReference": order_ref,
        "status": response_status,
        "time": now_ts,
        "signature": signature,
    }


# ── Monobank Webhook ──

@app.post("/webhook/monobank")
async def monobank_webhook(request: Request):
    """
    Monobank sends POST with JSON:
    {
      "type": "StatementItem",
      "data": {
        "account": "...",
        "statementItem": {
          "id": "...",
          "time": 1712345678,
          "description": "Від: Бондар Олена",
          "mcc": 4829,
          "originalMcc": 4829,
          "amount": 140000,      # in kopecks!
          "operationAmount": 140000,
          "currencyCode": 980,
          "commissionRate": 0,
          "cashbackAmount": 0,
          "balance": ...,
          "comment": "...",
          "counterEdrpou": "...",
          "counterIban": "...",
          "counterName": "БОНДАР ОЛЕНА"
        }
      }
    }
    """
    try:
        body = await request.json()
    except Exception:
        body = {}

    data = body.get("data", {})
    account_id = data.get("account", "")
    statement = data.get("statementItem", {})

    # Check if this account is in our selected accounts list
    mono_settings = await db_select(
        "bot_settings", columns="value",
        filters={"key": "monobank"}, maybe_single=True,
    )
    if mono_settings and isinstance(mono_settings, dict):
        val = mono_settings.get("value", {})
        if isinstance(val, str):
            try:
                val = json.loads(val)
            except Exception:
                val = {}
        selected_accounts = val.get("selected_accounts", [])
        if selected_accounts and account_id and account_id not in selected_accounts:
            print(f"[MONO] Skipping: account {account_id} not in selected accounts {selected_accounts}")
            return {"status": "ok", "skipped": "account not selected"}

    mono_id = statement.get("id", "")
    amount_kopecks = statement.get("amount", 0)
    amount = abs(amount_kopecks) / 100  # Convert from kopecks to UAH
    description = statement.get("description", "")
    counter_name = statement.get("counterName", "")
    counter_iban = statement.get("counterIban", "")
    comment = statement.get("comment", "")
    tx_time = statement.get("time", 0)

    # Only process incoming payments (positive amount)
    if amount_kopecks <= 0:
        return {"status": "ok", "skipped": "outgoing transaction"}

    # Skip internal/own transfers (between own accounts, interest, etc.)
    skip_patterns = [
        "з гривневого рахунку",
        "на гривневий рахунок",
        "з картки",
        "на картку",
        "власний рахунок",
        "між своїми",
        "відсотки",
        "кешбек",
        "cashback",
    ]
    desc_lower = description.lower()
    if any(pat in desc_lower for pat in skip_patterns) and not counter_iban:
        print(f"[MONO] Skipping internal transfer: {description}")
        return {"status": "ok", "skipped": "internal transfer"}

    # Extract payer name: prefer counterName, fallback to description parsing
    payer_name = counter_name
    if not payer_name and description:
        # Try to extract from "Від: Прізвище Ім'я" pattern
        match = re.search(r"(?:Від|від|From|from)[:\s]+(.+)", description)
        if match:
            payer_name = match.group(1).strip()
        else:
            # Skip if description looks like system text, not a person name
            if len(description) > 50 or any(c.isdigit() for c in description[:3]):
                payer_name = description[:50]
            else:
                payer_name = description

    print(f"[MONO] Webhook received: id={mono_id}, amount={amount}, payer={payer_name}")

    payment_date = datetime.utcfromtimestamp(tx_time).isoformat() if tx_time else datetime.utcnow().isoformat()

    payment_data = {
        "source": "monobank",
        "external_id": mono_id,
        "payer_name": payer_name,
        "amount": amount,
        "currency": "UAH",
        "status": "success",
        "raw_data": json.dumps(body),
        "payment_date": payment_date,
    }
    result = await db_insert("payments", payment_data)
    payment_id = None
    if isinstance(result, list) and result:
        payment_id = result[0].get("id")
    elif isinstance(result, dict):
        payment_id = result.get("id")

    # Auto-match
    match_result = None
    if payment_id and payer_name:
        match_result = await _auto_match_payment(payment_id, payer_name, amount)

    return {"status": "ok", "payment_id": payment_id, "match": match_result}


# ── Monobank: Setup Webhook via their API ──

@app.post("/api/monobank/setup-webhook")
async def setup_monobank_webhook(request: Request):
    """Register our webhook URL with Monobank personal API."""
    body = await request.json()
    token = body.get("token", "")
    webhook_url = body.get("webhook_url", "https://iseasy-crm-api.vercel.app/webhook/monobank")

    if not token:
        raise HTTPException(400, "Monobank API token is required")

    async with httpx.AsyncClient() as client:
        # Set webhook
        resp = await client.post(
            "https://api.monobank.ua/personal/webhook",
            json={"webHookUrl": webhook_url},
            headers={"X-Token": token},
        )

        if resp.status_code == 200:
            # Verify by getting client info
            info_resp = await client.get(
                "https://api.monobank.ua/personal/client-info",
                headers={"X-Token": token},
            )
            info = info_resp.json() if info_resp.status_code == 200 else {}
            client_name = info.get("name", "")
            accounts = info.get("accounts", [])
            # Find UAH account (currencyCode 980)
            uah_accounts = [
                {
                    "id": a.get("id", ""),
                    "balance": a.get("balance", 0) / 100,
                    "type": a.get("type", ""),
                    "maskedPan": (a.get("maskedPan", [None]) or [None])[0] or "",
                    "iban": a.get("iban", ""),
                }
                for a in accounts
                if a.get("currencyCode") == 980
            ]

            return {
                "status": "ok",
                "client_name": client_name,
                "accounts": len(uah_accounts),
                "accounts_list": uah_accounts,
                "webhook_set": True,
            }
        else:
            error_text = resp.text
            return {
                "status": "error",
                "code": resp.status_code,
                "error": error_text,
            }


@app.post("/api/monobank/accounts")
async def get_monobank_accounts(request: Request):
    """Get list of Monobank UAH accounts for account selection."""
    body = await request.json()
    token = body.get("token", "")
    if not token:
        raise HTTPException(400, "Monobank API token is required")

    async with httpx.AsyncClient() as client:
        info_resp = await client.get(
            "https://api.monobank.ua/personal/client-info",
            headers={"X-Token": token},
        )
        if info_resp.status_code != 200:
            return {"status": "error", "error": info_resp.text}
        info = info_resp.json()
        client_name = info.get("name", "")
        accounts = info.get("accounts", [])
        uah_accounts = [
            {
                "id": a.get("id", ""),
                "balance": a.get("balance", 0) / 100,
                "type": a.get("type", ""),
                "maskedPan": (a.get("maskedPan", [None]) or [None])[0] or "",
                "iban": a.get("iban", ""),
            }
            for a in accounts
            if a.get("currencyCode") == 980
        ]
        return {
            "status": "ok",
            "client_name": client_name,
            "accounts_list": uah_accounts,
        }


# ── Manual payment matching ──

@app.post("/api/payments/{payment_id}/match")
async def manual_match_payment(payment_id: int, request: Request):
    """Manually match a payment to an order or just to a conversation."""
    body = await request.json()
    order_id = body.get("order_id")
    conversation_id = body.get("conversation_id")

    # Get payment
    payment = await db_select("payments", filters={"id": payment_id}, single=True)
    amount = float(payment.get("amount", 0))

    if order_id:
        # Match to specific order
        order = await db_select("orders", filters={"id": order_id}, single=True)
        new_paid = float(order.get("paid", 0)) + amount
        order_total = float(order.get("total", 0))
        new_status = "confirmed" if new_paid >= order_total else order.get("status", "payment")
        conv_id = order.get("conversation_id") or conversation_id

        await db_update("payments", {
            "matched_order_id": order_id,
            "matched_conversation_id": conv_id,
            "is_matched": True,
        }, {"id": payment_id})

        await db_update("orders", {
            "paid": new_paid,
            "status": new_status,
            "updated_at": datetime.utcnow().isoformat(),
        }, {"id": order_id})

        return {
            "status": "ok",
            "payment_id": payment_id,
            "order_id": order_id,
            "new_paid": new_paid,
            "fully_paid": new_paid >= order_total,
        }
    elif conversation_id:
        # Match to conversation only (no order yet)
        await db_update("payments", {
            "matched_conversation_id": conversation_id,
            "is_matched": True,
        }, {"id": payment_id})

        return {
            "status": "ok",
            "payment_id": payment_id,
            "conversation_id": conversation_id,
            "amount": amount,
        }
    else:
        raise HTTPException(400, "order_id or conversation_id is required")


@app.post("/api/payments/{payment_id}/unmatch")
async def unmatch_payment(payment_id: int):
    """Remove payment matching — make it available again for linking."""
    payment = await db_select("payments", filters={"id": payment_id}, single=True)
    old_order_id = payment.get("matched_order_id")
    amount = float(payment.get("amount", 0))

    # Unset matching fields
    await db_update("payments", {
        "matched_order_id": None,
        "matched_conversation_id": None,
        "is_matched": False,
    }, {"id": payment_id})

    # If was matched to order, subtract amount from paid
    if old_order_id:
        order = await db_select("orders", filters={"id": old_order_id}, single=True)
        new_paid = max(0, float(order.get("paid", 0)) - amount)
        await db_update("orders", {
            "paid": new_paid,
            "status": "payment" if new_paid < float(order.get("total", 0)) else order.get("status"),
            "updated_at": datetime.utcnow().isoformat(),
        }, {"id": old_order_id})
        return {"status": "ok", "payment_id": payment_id, "order_id": old_order_id, "new_paid": new_paid}

    return {"status": "ok", "payment_id": payment_id}


# ── Get unmatched payments ──

@app.get("/api/payments/unmatched")
async def unmatched_payments():
    """Get payments that haven't been matched to an order yet."""
    return await db_select(
        "payments",
        filters={"is_matched": False},
        order="payment_date.desc",
        limit=50,
    )


# ── Suggest matching payments for a client by name ──

@app.get("/api/payments/suggest")
async def suggest_payments(client_name: str = "", conversation_id: int = 0, days: int = 3):
    """
    Get all payments for the last N days, sorted by relevance to client_name.
    Returns both matched and unmatched payments.
    """
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    # Use raw REST query to filter by date
    params = {
        "payment_date": f"gte.{since}",
        "order": "payment_date.desc",
        "limit": "200",
    }
    hdrs = _h()
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{REST_URL}/payments", params=params, headers=hdrs)
    all_payments = resp.json() if resp.status_code == 200 else []
    if not isinstance(all_payments, list):
        return []

    scored = []
    for p in all_payments:
        payer = p.get("payer_name", "")
        score = _fuzzy_match(client_name, payer) if client_name else 0
        scored.append({**p, "match_score": round(score, 2)})

    # Sort: best matches first, then by date desc
    scored.sort(key=lambda x: (-x["match_score"], x.get("payment_date", "")))
    return scored


# ── Get matched payments for a conversation ──

@app.get("/api/payments/by-conversation/{conversation_id}")
async def payments_by_conversation(conversation_id: int):
    """Get all payments matched to a specific conversation."""
    return await db_select(
        "payments",
        filters={"matched_conversation_id": conversation_id},
        order="payment_date.desc",
        limit=50,
    )


# ── Monobank: Sync historical statements ──

@app.post("/api/monobank/sync")
async def monobank_sync_statements(request: Request):
    """
    Fetch historical transactions from Monobank statement API
    for the last N days and insert new ones into payments table.
    GET /personal/statement/{account}/{from}/{to}
    """
    body = await request.json()
    days = body.get("days", 3)

    # Get monobank settings (token + selected_accounts)
    mono_settings = await db_select(
        "bot_settings", columns="value",
        filters={"key": "monobank"}, maybe_single=True,
    )
    if not mono_settings:
        raise HTTPException(400, "Monobank not configured. Save token in settings first.")

    val = mono_settings.get("value", {}) if isinstance(mono_settings, dict) else {}
    if isinstance(val, str):
        try:
            val = json.loads(val)
        except Exception:
            val = {}

    token = val.get("api_token", "")
    if not token:
        raise HTTPException(400, "Monobank API token not found in settings")

    selected_accounts = val.get("selected_accounts", [])

    # If no accounts selected, get all UAH accounts
    if not selected_accounts:
        async with httpx.AsyncClient() as client:
            info_resp = await client.get(
                "https://api.monobank.ua/personal/client-info",
                headers={"X-Token": token},
            )
            if info_resp.status_code == 200:
                info = info_resp.json()
                selected_accounts = [
                    a["id"] for a in info.get("accounts", [])
                    if a.get("currencyCode") == 980
                ]

    if not selected_accounts:
        return {"status": "error", "error": "No UAH accounts found"}

    now = int(datetime.utcnow().timestamp())
    from_time = now - (days * 86400)

    skip_patterns = [
        "з гривневого рахунку", "на гривневий рахунок",
        "з картки", "на картку", "власний рахунок",
        "між своїми", "відсотки", "кешбек", "cashback",
    ]

    total_synced = 0
    total_skipped = 0
    errors = []

    for account_id in selected_accounts:
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                url = f"https://api.monobank.ua/personal/statement/{account_id}/{from_time}/{now}"
                resp = await client.get(url, headers={"X-Token": token})

            if resp.status_code == 429:
                errors.append(f"Account {account_id[-4:]}: rate limited (wait 60s)")
                continue
            if resp.status_code != 200:
                errors.append(f"Account {account_id[-4:]}: HTTP {resp.status_code}")
                continue

            items = resp.json()
            if not isinstance(items, list):
                continue

            for stmt in items:
                amount_kopecks = stmt.get("amount", 0)
                # Only incoming payments
                if amount_kopecks <= 0:
                    total_skipped += 1
                    continue

                description = stmt.get("description", "")
                counter_name = stmt.get("counterName", "")
                counter_iban = stmt.get("counterIban", "")
                desc_lower = description.lower()

                # Skip internal transfers
                if any(pat in desc_lower for pat in skip_patterns) and not counter_iban:
                    total_skipped += 1
                    continue

                mono_id = stmt.get("id", "")
                amount = abs(amount_kopecks) / 100
                comment = stmt.get("comment", "")
                tx_time = stmt.get("time", 0)

                # Check if already exists by external_id
                existing = await db_select(
                    "payments", columns="id",
                    filters={"external_id": mono_id}, maybe_single=True,
                )
                if existing and (isinstance(existing, dict) and existing.get("id")):
                    total_skipped += 1
                    continue

                # Extract payer name
                payer_name = counter_name
                if not payer_name and description:
                    match = re.search(r"(?:Від|від|From|from)[:\s]+(.+)", description)
                    if match:
                        payer_name = match.group(1).strip()
                    else:
                        payer_name = description[:50]

                payment_date = datetime.utcfromtimestamp(tx_time).isoformat() if tx_time else datetime.utcnow().isoformat()

                payment_data = {
                    "source": "monobank",
                    "external_id": mono_id,
                    "payer_name": payer_name,
                    "amount": amount,
                    "currency": "UAH",
                    "status": "success",
                    "raw_data": json.dumps({"statementItem": stmt, "account": account_id}),
                    "payment_date": payment_date,
                }
                try:
                    result = await db_insert("payments", payment_data)
                    total_synced += 1
                    # Auto-match
                    pid = None
                    if isinstance(result, list) and result:
                        pid = result[0].get("id")
                    elif isinstance(result, dict):
                        pid = result.get("id")
                    if pid and payer_name:
                        await _auto_match_payment(pid, payer_name, amount)
                except Exception as e:
                    print(f"[MONO SYNC] Insert error: {e}")

        except Exception as e:
            errors.append(f"Account {account_id[-4:]}: {str(e)}")

    # Also sync WayForPay if configured
    wfp_synced, wfp_errors = await _sync_wayforpay(days)
    total_synced += wfp_synced

    return {
        "status": "ok",
        "synced": total_synced,
        "skipped": total_skipped,
        "errors": errors + wfp_errors,
    }


async def _sync_wayforpay(days: int = 3):
    """Fetch WayForPay transaction list for the last N days."""
    wfp_settings = await db_select(
        "bot_settings", columns="value",
        filters={"key": "wayforpay"}, maybe_single=True,
    )
    if not wfp_settings:
        return 0, []

    val = wfp_settings.get("value", {}) if isinstance(wfp_settings, dict) else {}
    if isinstance(val, str):
        try:
            val = json.loads(val)
        except Exception:
            val = {}

    merchant_account = val.get("merchant_account", "")
    merchant_secret = val.get("merchant_secret", "")
    if not merchant_account or not merchant_secret:
        return 0, ["WFP: merchant_account or merchant_secret not configured"]

    now = int(datetime.utcnow().timestamp())
    date_begin = now - (days * 86400)

    # WFP API: transactionList
    # Signature = HMAC_MD5(merchantSecret, merchantAccount;dateBegin;dateEnd)
    sign_string = f"{merchant_account};{date_begin};{now}"
    signature = hmac.new(
        merchant_secret.encode("utf-8"),
        sign_string.encode("utf-8"),
        hashlib.md5,
    ).hexdigest()

    request_body = {
        "transactionType": "TRANSACTION_LIST",
        "merchantAccount": merchant_account,
        "merchantSignature": signature,
        "dateBegin": date_begin,
        "dateEnd": now,
    }

    synced = 0
    errors = []

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.wayforpay.com/api",
                json=request_body,
            )

        if resp.status_code != 200:
            return 0, [f"WFP: HTTP {resp.status_code}"]

        data = resp.json()
        reason_code = data.get("reasonCode")
        if reason_code and reason_code != 1100:
            reason = data.get("reason", "unknown")
            return 0, [f"WFP: {reason} (code {reason_code})"]

        transactions = data.get("transactionList", [])
        if not isinstance(transactions, list):
            return 0, []

        for tx in transactions:
            tx_status = tx.get("transactionStatus", "")
            if tx_status != "Approved":
                continue

            order_ref = tx.get("orderReference", "")
            amount = float(tx.get("amount", 0))
            payer_name = tx.get("clientName", "") or tx.get("client_name", "")
            currency = tx.get("currency", "UAH")

            # Check if already exists
            existing = await db_select(
                "payments", columns="id",
                filters={"external_id": order_ref}, maybe_single=True,
            )
            if existing and (isinstance(existing, dict) and existing.get("id")):
                continue

            # Payment date
            created = tx.get("createdDate", "") or tx.get("processingDate", "")
            if created and isinstance(created, (int, float)):
                payment_dt = datetime.utcfromtimestamp(created).isoformat()
            elif created and isinstance(created, str) and created.isdigit():
                payment_dt = datetime.utcfromtimestamp(int(created)).isoformat()
            else:
                payment_dt = datetime.utcnow().isoformat()

            payment_data = {
                "source": "wayforpay",
                "external_id": order_ref,
                "payer_name": payer_name,
                "amount": amount,
                "currency": currency,
                "status": "success",
                "raw_data": json.dumps(tx),
                "payment_date": payment_dt,
            }
            try:
                result = await db_insert("payments", payment_data)
                synced += 1
                pid = None
                if isinstance(result, list) and result:
                    pid = result[0].get("id")
                elif isinstance(result, dict):
                    pid = result.get("id")
                if pid and payer_name:
                    await _auto_match_payment(pid, payer_name, amount)
            except Exception as e:
                print(f"[WFP SYNC] Insert error: {e}")

    except Exception as e:
        errors.append(f"WFP: {str(e)}")

    return synced, errors


# ═══════════════════════════════════════
#  НОВА ПОШТА — API ІНТЕГРАЦІЯ
# ═══════════════════════════════════════

NP_API_URL = "https://api.novaposhta.ua/v2.0/json/"


async def _np_call(api_key: str, model: str, method: str, props: dict = None):
    """Universal Nova Poshta API v2.0 caller."""
    payload = {
        "apiKey": api_key,
        "modelName": model,
        "calledMethod": method,
        "methodProperties": props or {},
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(NP_API_URL, json=payload)
    data = resp.json()
    if not data.get("success"):
        errors = data.get("errors", [])
        raise HTTPException(400, detail=f"Nova Poshta API error: {'; '.join(errors)}")
    return data.get("data", [])


async def _get_np_key() -> str:
    """Get Nova Poshta API key from bot_settings."""
    settings = await db_select(
        "bot_settings", columns="value",
        filters={"key": "nova_poshta"}, maybe_single=True,
    )
    if not settings or not isinstance(settings, dict):
        raise HTTPException(400, "Nova Poshta не налаштовано. Збережіть API ключ в налаштуваннях.")
    val = settings.get("value", {})
    api_key = val.get("apiKey", "")
    if not api_key:
        raise HTTPException(400, "API ключ Нова Пошта не знайдено в налаштуваннях")
    return api_key


@app.post("/api/nova-poshta/test-key")
async def np_test_key(request: Request):
    """Test Nova Poshta API key validity."""
    body = await request.json()
    api_key = body.get("apiKey", "")
    if not api_key:
        raise HTTPException(400, "API ключ не вказано")
    try:
        result = await _np_call(api_key, "Counterparty", "getCounterparties", {
            "CounterpartyProperty": "Sender",
            "Page": "1",
        })
        sender = result[0] if result else {}
        return {
            "ok": True,
            "sender": {
                "ref": sender.get("Ref", ""),
                "description": sender.get("Description", ""),
                "city": sender.get("City", ""),
                "ownershipForm": sender.get("OwnershipForm", ""),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/nova-poshta/sender-info")
async def np_sender_info():
    """Get sender counterparty info (name, ref, contact persons, addresses)."""
    api_key = await _get_np_key()
    counterparties = await _np_call(api_key, "Counterparty", "getCounterparties", {
        "CounterpartyProperty": "Sender",
        "Page": "1",
    })
    if not counterparties:
        return {"counterparties": [], "addresses": [], "contacts": []}

    sender_ref = counterparties[0].get("Ref", "")

    # Get sender addresses
    addresses = await _np_call(api_key, "Counterparty", "getCounterpartyAddresses", {
        "Ref": sender_ref,
        "CounterpartyProperty": "Sender",
    })

    # Get sender contact persons
    contacts = await _np_call(api_key, "Counterparty", "getCounterpartyContactPersons", {
        "Ref": sender_ref,
        "Page": "1",
    })

    return {
        "counterparties": counterparties,
        "addresses": addresses,
        "contacts": contacts,
    }


@app.get("/api/nova-poshta/cities")
async def np_search_cities(q: str = "", limit: int = 20):
    """Search cities by name."""
    api_key = await _get_np_key()
    result = await _np_call(api_key, "Address", "searchSettlements", {
        "CityName": q,
        "Limit": str(limit),
    })
    # searchSettlements returns nested Addresses array
    addresses = []
    for item in result:
        for addr in item.get("Addresses", []):
            addresses.append({
                "ref": addr.get("DeliveryCity", addr.get("Ref", "")),
                "name": addr.get("MainDescription", "") or addr.get("Present", ""),
                "area": addr.get("Area", ""),
                "region": addr.get("Region", ""),
                "present": addr.get("Present", ""),
            })
    return addresses


@app.get("/api/nova-poshta/warehouses")
async def np_search_warehouses(city_ref: str = "", q: str = "", limit: int = 500):
    """Search warehouses/postomat by city ref."""
    api_key = await _get_np_key()
    props = {"Limit": str(limit)}
    if city_ref:
        props["CityRef"] = city_ref
    if q:
        props["FindByString"] = q
    result = await _np_call(api_key, "Address", "getWarehouses", props)
    return [
        {
            "ref": w.get("Ref", ""),
            "number": w.get("Number", ""),
            "description": w.get("Description", ""),
            "shortAddress": w.get("ShortAddress", ""),
            "phone": w.get("Phone", ""),
            "typeOfWarehouse": w.get("TypeOfWarehouse", ""),
            "cityRef": w.get("CityRef", ""),
            "cityDescription": w.get("CityDescription", ""),
            "maxWeight": w.get("TotalMaxWeightAllowed", ""),
            "schedule": {
                "mon": w.get("Schedule", {}).get("Monday", ""),
                "sat": w.get("Schedule", {}).get("Saturday", ""),
                "sun": w.get("Schedule", {}).get("Sunday", ""),
            } if isinstance(w.get("Schedule"), dict) else {},
        }
        for w in result
    ]


@app.post("/api/nova-poshta/create-ttn")
async def np_create_ttn(request: Request):
    """Create an internet document (TTN) via Nova Poshta API."""
    body = await request.json()
    api_key = await _get_np_key()

    # Get sender settings from DB
    np_settings = await db_select(
        "bot_settings", columns="value",
        filters={"key": "nova_poshta"}, maybe_single=True,
    )
    val = np_settings.get("value", {}) if np_settings else {}

    sender_ref = val.get("senderRef", "")
    sender_address = val.get("senderAddressRef", "")
    sender_contact = val.get("senderContactRef", "")
    sender_phone = val.get("senderPhone", "")

    if not all([sender_ref, sender_address, sender_contact]):
        raise HTTPException(400, "Налаштуйте відправника в налаштуваннях Нова Пошта (контрагент, адреса, контакт)")

    # Required fields from request
    recipient_name = body.get("recipientName", "")
    recipient_phone = body.get("recipientPhone", "")
    recipient_city_ref = body.get("recipientCityRef", "")
    recipient_warehouse_ref = body.get("recipientWarehouseRef", "")
    weight = body.get("weight", "0.5")
    cost = body.get("cost", 0)
    description = body.get("description", "Взуття")
    payer_type = body.get("payerType", "Recipient")
    payment_method = body.get("paymentMethod", "Cash")
    cargo_type = body.get("cargoType", "Parcel")
    seats_amount = body.get("seatsAmount", "1")

    # Backward delivery (наложка)
    backward_delivery = []
    cod_amount = body.get("codAmount", 0)
    if cod_amount and float(cod_amount) > 0:
        backward_delivery = [{
            "PayerType": "Recipient",
            "CargoType": "Money",
            "RedeliveryString": str(cod_amount),
        }]

    props = {
        "PayerType": payer_type,
        "PaymentMethod": payment_method,
        "DateTime": datetime.utcnow().strftime("%d.%m.%Y"),
        "CargoType": cargo_type,
        "Weight": str(weight),
        "ServiceType": "WarehouseWarehouse",
        "SeatsAmount": str(seats_amount),
        "Description": description,
        "Cost": str(cost),
        "CitySender": val.get("senderCityRef", ""),
        "Sender": sender_ref,
        "SenderAddress": sender_address,
        "ContactSender": sender_contact,
        "SendersPhone": sender_phone,
        "CityRecipient": recipient_city_ref,
        "Recipient": "",
        "RecipientAddress": recipient_warehouse_ref,
        "ContactRecipient": "",
        "RecipientsPhone": recipient_phone,
        "NewAddress": "1",
    }

    # If recipient is a private person, create on the fly
    if recipient_name and recipient_phone:
        # Split name: Прізвище Ім'я По-батькові
        name_parts = recipient_name.strip().split()
        last_name = name_parts[0] if name_parts else ""
        first_name = name_parts[1] if len(name_parts) > 1 else ""
        middle_name = name_parts[2] if len(name_parts) > 2 else ""

        # Create recipient counterparty
        try:
            cp_result = await _np_call(api_key, "Counterparty", "save", {
                "FirstName": first_name,
                "MiddleName": middle_name,
                "LastName": last_name,
                "Phone": recipient_phone,
                "Email": "",
                "CounterpartyType": "PrivatePerson",
                "CounterpartyProperty": "Recipient",
            })
            if cp_result:
                props["Recipient"] = cp_result[0].get("Ref", "")
                contact_persons = cp_result[0].get("ContactPerson", {}).get("data", [])
                if contact_persons:
                    props["ContactRecipient"] = contact_persons[0].get("Ref", "")
        except Exception as e:
            print(f"[NP] Failed to create recipient counterparty: {e}")
            raise HTTPException(400, f"Помилка створення отримувача: {e}")

    if backward_delivery:
        props["BackwardDeliveryData"] = backward_delivery

    result = await _np_call(api_key, "InternetDocument", "save", props)

    if result:
        doc = result[0]
        ttn = doc.get("IntDocNumber", "")
        doc_ref = doc.get("Ref", "")
        estimated_cost = doc.get("CostOnSite", 0)

        # Update order with TTN if order_id provided
        order_id = body.get("orderId")
        if order_id:
            await db_update("orders", {
                "ttn": ttn,
                "np_doc_ref": doc_ref,
                "status": "shipped",
                "updated_at": datetime.utcnow().isoformat(),
            }, {"id": order_id})

        return {
            "ok": True,
            "ttn": ttn,
            "ref": doc_ref,
            "estimatedCost": estimated_cost,
            "deliveryDate": doc.get("EstimatedDeliveryDate", ""),
        }

    raise HTTPException(500, "Не вдалося створити ТТН")


@app.post("/api/nova-poshta/track")
async def np_track(request: Request):
    """Track one or multiple TTN numbers."""
    body = await request.json()
    api_key = await _get_np_key()

    ttns = body.get("ttns", [])
    if isinstance(ttns, str):
        ttns = [ttns]
    if not ttns:
        ttn = body.get("ttn", "")
        if ttn:
            ttns = [ttn]

    if not ttns:
        raise HTTPException(400, "Вкажіть номер ТТН для відстеження")

    documents = [{"DocumentNumber": t, "Phone": ""} for t in ttns]

    result = await _np_call(api_key, "TrackingDocument", "getStatusDocuments", {
        "Documents": documents,
    })

    return [
        {
            "ttn": item.get("Number", ""),
            "status": item.get("Status", ""),
            "statusCode": item.get("StatusCode", ""),
            "statusDescription": item.get("Status", ""),
            "warehouseSender": item.get("WarehouseSender", ""),
            "warehouseRecipient": item.get("WarehouseRecipient", ""),
            "cityRecipient": item.get("CityRecipient", ""),
            "recipientName": item.get("RecipientFullName", ""),
            "scheduledDelivery": item.get("ScheduledDeliveryDate", ""),
            "actualDelivery": item.get("ActualDeliveryDate", ""),
            "weight": item.get("DocumentWeight", ""),
            "cost": item.get("DocumentCost", ""),
            "redeliverySum": item.get("RedeliverySum", ""),
            "storagePrice": item.get("StoragePrice", ""),
            "daysInStorage": item.get("DaysStorageCargo", ""),
        }
        for item in result
    ]


@app.post("/api/nova-poshta/estimate-cost")
async def np_estimate_cost(request: Request):
    """Estimate delivery cost."""
    body = await request.json()
    api_key = await _get_np_key()

    np_settings = await db_select(
        "bot_settings", columns="value",
        filters={"key": "nova_poshta"}, maybe_single=True,
    )
    val = np_settings.get("value", {}) if np_settings else {}

    result = await _np_call(api_key, "InternetDocument", "getDocumentPrice", {
        "CitySender": val.get("senderCityRef", ""),
        "CityRecipient": body.get("recipientCityRef", ""),
        "Weight": str(body.get("weight", "0.5")),
        "ServiceType": "WarehouseWarehouse",
        "Cost": str(body.get("cost", 500)),
        "CargoType": "Parcel",
        "SeatsAmount": "1",
    })

    if result:
        return {
            "cost": result[0].get("Cost", 0),
            "estimatedDelivery": result[0].get("EstimatedDeliveryDate", ""),
        }
    return {"cost": 0, "estimatedDelivery": ""}


# ═══════════════════════════════════════
#  AI AGENT LAYER  (/api/agent/*)
# ═══════════════════════════════════════
# Тонкий gate + audit для Claude tool use.
# Требует AGENT_API_TOKEN в env. См. api/agent.py и migrations/001_agent_tables.sql
try:
    from .agent import router as agent_router, _run_agent_loop_internal
    app.include_router(agent_router)
except Exception as _agent_err:
    print(f"[agent router] not loaded: {_agent_err}")
    _run_agent_loop_internal = None


# ═══════════════════════════════════════
#  AI AGENT: auto-reply from Instagram webhook
# ═══════════════════════════════════════

async def maybe_agent_reply(sender_id: str, conversation_id: int, text: str):
    """
    Вызывается из process_incoming_message ВМЕСТО maybe_auto_reply.
    Логика:
      1. Проверяет настройку agent_enabled в bot_settings
      2. Если агент выключен → fallback на шаблонный maybe_auto_reply
      3. Если агент включён → собирает контекст → вызывает Claude loop
      4. Агент сам решает: отвечать (auto), pending, или create_task
    """
    # Проверяем настройки агента
    agent_settings = await db_select(
        "bot_settings", columns="value",
        filters={"key": "agent_config"}, maybe_single=True,
    )
    agent_cfg = (agent_settings or {}).get("value") or {}

    if not agent_cfg.get("enabled", False):
        # Агент выключен — старый шаблонный автоответ
        await maybe_auto_reply(sender_id, conversation_id)
        return

    if _run_agent_loop_internal is None:
        print("[agent_reply] agent module not loaded, fallback to auto_reply")
        await maybe_auto_reply(sender_id, conversation_id)
        return

    # Собираем контекст для агента
    # Последние сообщения диалога
    recent_msgs = await db_select(
        "messages",
        columns="direction,content,message_type,created_at",
        filters={"conversation_id": conversation_id},
        order="created_at.desc",
        limit=10,
    )
    recent_msgs = list(reversed(recent_msgs or []))

    # Данные клиента
    conv = await db_select("conversations", filters={"id": conversation_id}, maybe_single=True)
    client_name = (conv or {}).get("client_name", "")
    ig_username = (conv or {}).get("instagram_username", "")
    funnel = (conv or {}).get("funnel", "new")

    # Формируем промпт с контекстом
    context_lines = []
    context_lines.append(f"Диалог #{conversation_id} | Клиент: {client_name} (@{ig_username}) | Воронка: {funnel}")
    context_lines.append("Последние сообщения:")
    for m in recent_msgs:
        direction = "Клиент" if m.get("direction") == "incoming" else "IS EASY"
        content = m.get("content", "")[:300]
        msg_type = m.get("message_type", "text")
        if msg_type != "text" and not content:
            content = f"[{msg_type}]"
        context_lines.append(f"  {direction}: {content}")

    context_lines.append(f"\nНовое сообщение от клиента: {text}")
    context_lines.append("Обработай это сообщение: определи намерение, используй инструменты и ответь клиенту.")

    prompt = "\n".join(context_lines)

    # Параметры из настроек
    model = agent_cfg.get("model", "claude-sonnet-4-20250514")
    max_turns = int(agent_cfg.get("max_turns", 6))

    try:
        result = await _run_agent_loop_internal(prompt, model=model, max_turns=max_turns)
        print(f"[agent_reply] conv={conversation_id} turns={result.get('turns_used')} "
              f"tokens_in={result.get('usage',{}).get('input_tokens',0)} "
              f"tokens_out={result.get('usage',{}).get('output_tokens',0)}")
    except Exception as e:
        print(f"[agent_reply] ERROR conv={conversation_id}: {e}")
        # Fallback: шаблонный автоответ если агент упал
        await maybe_auto_reply(sender_id, conversation_id)



# ─────────────────────────────────────────────────────────────
#  TEST CHAT API — POST /api/test-chat
# ─────────────────────────────────────────────────────────────

@app.post("/api/test-chat")
async def test_chat(request: Request, authorization: Optional[str] = Header(None)):
    token = ""
    if authorization:
        token = authorization.replace("Bearer ", "").strip()
    agent_token = os.environ.get("AGENT_API_TOKEN", "").strip()
    if agent_token and token != agent_token:
        raise HTTPException(status_code=403, detail="Invalid token")

    body = await request.json()
    text = (body.get("text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Empty text")

    client_name = body.get("client_name") or "Test Client"
    session_id = body.get("session_id") or "test_default"
    test_ig_id = "test_" + session_id

    conv = await db_select("conversations", filters={"instagram_user_id": test_ig_id}, maybe_single=True)

    if conv:
        conversation_id = conv["id"]
        await db_update("conversations", {
            "last_message_text": text[:200],
            "last_message_at": datetime.utcnow().isoformat(),
            "unread_count": conv.get("unread_count", 0) + 1,
            "updated_at": datetime.utcnow().isoformat(),
        }, {"id": conversation_id})
    else:
        new_conv = await db_insert("conversations", {
            "instagram_user_id": test_ig_id,
            "instagram_username": "test_" + session_id[:8],
            "client_name": client_name,
            "avatar_url": "",
            "last_message_text": text[:200],
            "last_message_at": datetime.utcnow().isoformat(),
            "unread_count": 1,
        })
        conversation_id = new_conv[0]["id"]

    await db_insert("messages", {
        "conversation_id": conversation_id,
        "instagram_message_id": "test_" + str(int(datetime.utcnow().timestamp() * 1000)),
        "direction": "incoming",
        "message_type": "text",
        "content": text,
    })

    if _run_agent_loop_internal is None:
        return {"error": "Agent module not loaded", "conversation_id": conversation_id}

    recent_msgs = await db_select(
        "messages", columns="direction,content,message_type,created_at",
        filters={"conversation_id": conversation_id}, order="created_at.desc", limit=10,
    )
    recent_msgs = list(reversed(recent_msgs or []))

    conv_data = await db_select("conversations", filters={"id": conversation_id}, maybe_single=True)
    ig_user = (conv_data or {}).get("instagram_username", "")
    funnel = (conv_data or {}).get("funnel", "new")

    lines = [f"Dialog #{conversation_id} | Client: {client_name} (@{ig_user}) | Funnel: {funnel}", "Recent messages:"]
    for m in recent_msgs:
        d = "Client" if m.get("direction") == "incoming" else "IS EASY"
        c = m.get("content", "")[:300]
        t = m.get("message_type", "text")
        if t != "text" and not c:
            c = "[" + t + "]"
        lines.append("  " + d + ": " + c)
    lines.append("")
    lines.append("New message from client: " + text)
    lines.append("Process this message: identify intent, use tools, reply to client.")

    prompt = chr(10).join(lines)

    try:
        agent_settings = await db_select("bot_settings", columns="value", filters={"key": "agent_config"}, maybe_single=True)
        agent_cfg = (agent_settings or {}).get("value") or {}
    except Exception:
        agent_cfg = {}

    model = agent_cfg.get("model", "claude-sonnet-4-20250514")
    max_turns = int(agent_cfg.get("max_turns", 6))

    try:
        result = await _run_agent_loop_internal(prompt, model=model, max_turns=max_turns)
        result["conversation_id"] = conversation_id

        agent_text = result.get("final_text", "")
        if agent_text:
            await db_insert("messages", {
                "conversation_id": conversation_id,
                "instagram_message_id": "agent_" + str(int(datetime.utcnow().timestamp() * 1000)),
                "direction": "outgoing",
                "message_type": "text",
                "content": agent_text,
            })
            await db_update("conversations", {
                "last_message_text": agent_text[:200],
                "last_message_at": datetime.utcnow().isoformat(),
            }, {"id": conversation_id})
        return result
    except Exception as e:
        return {"error": str(e), "conversation_id": conversation_id}



from fastapi.responses import HTMLResponse

@app.get("/test-chat", response_class=HTMLResponse)
async def test_chat_page():
    h = "<!DOCTYPE html><html><head><meta charset=utf-8><title>IS EASY Agent Test</title>"
    h += "<style>*{margin:0;padding:0;box-sizing:border-box}"
    h += "body{font-family:system-ui;background:#0f172a;color:#e2e8f0;height:100vh;display:flex;flex-direction:column}"
    h += ".h{background:#1e293b;padding:14px 20px;display:flex;align-items:center;gap:12px;border-bottom:1px solid #334155}"
    h += ".h h1{font-size:17px;color:#818cf8}.h .b{background:#4f46e5;color:#fff;font-size:11px;padding:2px 8px;border-radius:12px}"
    h += ".h .s{margin-left:auto;font-size:12px;color:#64748b}"
    h += ".c{background:#1e293b;padding:8px 20px;display:flex;align-items:center;gap:10px;border-bottom:1px solid #334155;font-size:12px;flex-wrap:wrap}"
    h += ".c label{color:#94a3b8}.c input{background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:5px 10px;border-radius:6px;font-size:12px}"
    h += ".c button{background:none;border:1px solid #334155;color:#94a3b8;padding:4px 10px;border-radius:6px;cursor:pointer;font-size:11px}"
    h += "#ch{flex:1;overflow-y:auto;padding:20px;display:flex;flex-direction:column;gap:10px}"
    h += ".m{max-width:75%;padding:12px 16px;border-radius:16px;font-size:14px;line-height:1.5}"
    h += ".u{align-self:flex-end;background:#4f46e5;color:#fff;border-bottom-right-radius:4px}"
    h += ".a{align-self:flex-start;background:#1e293b;border:1px solid #334155;border-bottom-left-radius:4px}"
    h += ".sy{align-self:center;background:#334155;color:#94a3b8;font-size:11px;padding:6px 14px;border-radius:10px}"
    h += ".lb{font-size:10px;color:#818cf8;font-weight:600;margin-bottom:4px}"
    h += ".mt{margin-top:6px;font-size:10px;color:#475569;display:flex;gap:10px;flex-wrap:wrap}"
    h += ".ib{background:#1e293b;padding:14px 20px;display:flex;gap:10px;border-top:1px solid #334155}"
    h += ".ib input{flex:1;background:#0f172a;border:1px solid #334155;color:#e2e8f0;padding:12px 16px;border-radius:12px;font-size:14px;outline:none}"
    h += ".ib button{background:#4f46e5;color:#fff;border:none;padding:12px 24px;border-radius:12px;font-size:14px;font-weight:600;cursor:pointer}"
    h += ".ib button:disabled{background:#475569}</style></head><body>"
    h += "<div class=h><h1>IS EASY AI Agent</h1><span class=b>TEST</span><span class=s id=st>Ready</span></div>"
    h += "<div class=c><label>Token:</label><input id=tk placeholder=\"Agent API Token\" style=width:240px>"
    h += " <label>Name:</label><input id=cn value=\"Test Client\" style=width:120px>"
    h += " <button onclick=nc()>New</button></div>"
    h += "<div id=ch></div>"
    h += "<div class=ib><input id=inp placeholder=\"Write as customer...\" onkeydown=\"if(event.key==='Enter')go()\">"
    h += "<button id=sb onclick=go()>Send</button></div>"
    h += "<script>"
    h += "var A=location.origin,ch=document.getElementById('ch'),inp=document.getElementById('inp'),"
    h += "sb=document.getElementById('sb'),st=document.getElementById('st'),sid='t_'+Date.now(),cid=null;"
    h += "function am(r,t,x){var d=document.createElement('div');d.className='m '+r;"
    h += "if(r==='a'){var v='<div class=lb>AI Agent</div>'+t.replace(/\\n/g,'<br>');"
    h += "if(x){v+='<div class=mt>';if(x.conversation_id)v+='<span>Conv #'+x.conversation_id+'</span>';"
    h += "if(x.turns_used)v+='<span>'+x.turns_used+' turns</span>';if(x.usage)v+='<span>'+(x.usage.input_tokens||0)+'/'+(x.usage.output_tokens||0)+'</span>';v+='</div>';}"
    h += "d.innerHTML=v;}else d.textContent=t;ch.appendChild(d);ch.scrollTop=ch.scrollHeight;}"
    h += "function go(){var t=inp.value.trim();if(!t)return;"
    h += "var tk=document.getElementById('tk').value.trim();if(!tk){am('sy','Enter token');return;}"
    h += "var cn=document.getElementById('cn').value.trim()||'Test';"
    h += "inp.value='';am('u',t);sb.disabled=true;st.textContent='Thinking...';"
    h += "fetch(A+'/api/test-chat',{method:'POST',headers:{'Content-Type':'application/json','Authorization':'Bearer '+tk},"
    h += "body:JSON.stringify({text:t,client_name:cn,session_id:sid})})"
    h += ".then(function(r){if(!r.ok)return r.text().then(function(e){am('sy','Error '+r.status+': '+e);sb.disabled=false;st.textContent='Error';});"
    h += "return r.json().then(function(d){if(d.conversation_id)cid=d.conversation_id;"
    h += "if(d.error){am('sy','Error: '+d.error);sb.disabled=false;st.textContent='Error';return;}"
    h += "am('a',d.final_text||'(empty)',d);sb.disabled=false;st.textContent='Ready';st.style.color='#4ade80';inp.focus();});})"
    h += ".catch(function(e){am('sy','Error: '+e.message);sb.disabled=false;st.textContent='Error';});"
    h += "}"
    h += "function nc(){ch.innerHTML='';sid='t_'+Date.now();cid=null;am('sy','New dialog');}"
    h += "am('sy','Enter Agent API Token and write as customer');"
    h += "</script></body></html>"
    return h
