"""
IS EASY CRM — AI Agent layer (/api/agent/*)

Тонкая обёртка над существующими эндпоинтами CRM для безопасного
вызова из Claude (tool use). Делает три вещи:

  1. Аутентификация по Bearer-токену (AGENT_API_TOKEN из env).
  2. Gate: для каждого tool решает autopilot/confirm (гибридный режим
     с порогами — см. TOOL_MODES и HYBRID_RESOLVERS).
  3. Audit: всё пишется в agent_actions. Confirm-действия кладутся
     в pending_actions и ждут одобрения менеджера в UI.

Подключается в api/index.py одной строкой:
    from .agent import router as agent_router
    app.include_router(agent_router)

Endpoints:
    POST /api/agent/execute        — главный вход: {tool, params} → результат или pending
    GET  /api/agent/tools          — список доступных tools (для отладки)
    GET  /api/agent/pending        — очередь на подтверждение
    POST /api/agent/pending/{id}/approve
    POST /api/agent/pending/{id}/reject
    GET  /api/agent/actions        — последние N записей лога
"""
from __future__ import annotations

import os
import re
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Any, Callable, Optional

from fastapi import APIRouter, Header, HTTPException, Request

# Реюзаем инфраструктуру из основного модуля
from .index import (
    db_select,
    db_insert,
    db_update,
    send_instagram_message,
    REST_URL,
    HEADERS,
)

import httpx


# ─────────────────────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────────────────────
AGENT_API_TOKEN = os.getenv("AGENT_API_TOKEN", "")
PENDING_TTL_MINUTES = int(os.getenv("AGENT_PENDING_TTL_MIN", "60"))

# Пороги гибридного режима (можно править без кода через env)
HYBRID_ORDER_AUTO_MAX_UAH = float(os.getenv("AGENT_ORDER_AUTO_MAX_UAH", "2000"))
HYBRID_ORDER_AUTO_MAX_QTY = int(os.getenv("AGENT_ORDER_AUTO_MAX_QTY", "2"))
HYBRID_MSG_AUTO_MAX_LEN = int(os.getenv("AGENT_MSG_AUTO_MAX_LEN", "250"))
HYBRID_PAYMENT_AUTO_MIN_CONF = float(os.getenv("AGENT_PAYMENT_AUTO_MIN_CONF", "0.95"))


router = APIRouter(prefix="/api/agent", tags=["agent"])


# ─────────────────────────────────────────────────────────────
#  Auth
# ─────────────────────────────────────────────────────────────
def _check_auth(authorization: Optional[str]):
    """Bearer-токен из env. Raise HTTPException если не совпал."""
    if not AGENT_API_TOKEN:
        raise HTTPException(
            status_code=503,
            detail="AGENT_API_TOKEN is not configured on the server",
        )
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token != AGENT_API_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid agent token")


# ─────────────────────────────────────────────────────────────
#  Gate: режимы выполнения для каждого tool
# ─────────────────────────────────────────────────────────────
TOOL_MODES: dict[str, str] = {
    # READ — всегда auto
    "find_conversation":          "auto",
    "get_conversation_messages":  "auto",
    "search_products":            "auto",
    "get_product_availability":   "auto",
    "list_unmatched_payments":    "auto",
    "suggest_payment_match":      "auto",
    "np_resolve_city_warehouse":  "auto",
    "np_estimate_delivery_cost":  "auto",
    "np_track_ttn":               "auto",
    "get_stats":                  "auto",

    # WRITE (безопасные) — auto
    "find_or_create_client":      "auto",
    "update_client_card":         "auto",
    "update_conversation":        "auto",
    "create_task":                "auto",
    "notify_order_status":        "auto",

    # WRITE (гибрид)
    "send_message_to_client":     "hybrid",
    "create_order":               "hybrid",
    "update_order_status":        "hybrid",
    "match_payment_to_order":     "hybrid",

    # WRITE (всегда подтверждение)
    "update_order_details":       "confirm",
    "np_create_ttn":              "confirm",
    "propose_automation":         "confirm",
}


# Возвращаем (mode, reason_if_confirm)
_RISKY_WORDS = [
    "грн", "uah", "скид", "повернен", "возвр", "гарант",
    "обіц", "обеща", "компенс", "вибач", "извин",
]


def _hybrid_send_message(params: dict) -> tuple[str, str]:
    text = (params.get("text") or "")
    low = text.lower()
    if len(text) > HYBRID_MSG_AUTO_MAX_LEN:
        return "confirm", f"сообщение длиннее {HYBRID_MSG_AUTO_MAX_LEN} символов"
    if any(w in low for w in _RISKY_WORDS):
        return "confirm", "сообщение содержит рискованные слова (цены/возврат/обещание)"
    for num in re.findall(r"\d+", text):
        if int(num) > 100:
            return "confirm", "сообщение содержит число >100 (вероятно цена/сумма)"
    intent = params.get("intent", "")
    if intent in ("price_quote", "apology", "order_confirm"):
        return "confirm", f"intent={intent} требует согласования"
    return "auto", ""


def _hybrid_create_order(params: dict) -> tuple[str, str]:
    qty = int(params.get("quantity") or 1)
    if qty > HYBRID_ORDER_AUTO_MAX_QTY:
        return "confirm", f"quantity={qty} > лимита {HYBRID_ORDER_AUTO_MAX_QTY}"
    if not params.get("client_id"):
        return "confirm", "нет client_id (новый/неизвестный клиент)"
    price = params.get("price")
    if price is not None:
        try:
            if float(price) * qty >= HYBRID_ORDER_AUTO_MAX_UAH:
                return "confirm", f"сумма >= {HYBRID_ORDER_AUTO_MAX_UAH} UAH"
        except (TypeError, ValueError):
            return "confirm", "некорректная цена"
    return "auto", ""


def _hybrid_update_order_status(params: dict) -> tuple[str, str]:
    auto_targets = {"new", "confirmed", "paid"}
    ns = params.get("new_status", "")
    if ns in auto_targets:
        return "auto", ""
    return "confirm", f"переход в статус '{ns}' требует подтверждения"


def _hybrid_match_payment(params: dict) -> tuple[str, str]:
    try:
        conf = float(params.get("confidence") or 0)
    except (TypeError, ValueError):
        conf = 0
    if conf >= HYBRID_PAYMENT_AUTO_MIN_CONF:
        return "auto", ""
    return "confirm", f"confidence={conf:.2f} < {HYBRID_PAYMENT_AUTO_MIN_CONF}"


HYBRID_RESOLVERS: dict[str, Callable[[dict], tuple[str, str]]] = {
    "send_message_to_client": _hybrid_send_message,
    "create_order": _hybrid_create_order,
    "update_order_status": _hybrid_update_order_status,
    "match_payment_to_order": _hybrid_match_payment,
}


def resolve_mode(tool_name: str, params: dict) -> tuple[str, str]:
    base = TOOL_MODES.get(tool_name)
    if base is None:
        return "confirm", "неизвестный tool — всегда подтверждение"
    if base == "hybrid":
        resolver = HYBRID_RESOLVERS.get(tool_name)
        if resolver:
            return resolver(params)
        return "confirm", "нет резолвера для hybrid"
    return base, ""


# ─────────────────────────────────────────────────────────────
#  Audit & pending queue
# ─────────────────────────────────────────────────────────────
async def log_action(
    tool_name: str,
    params: dict,
    mode: str,
    status: str,
    result: Any = None,
    error: str = "",
    pending_id: Optional[int] = None,
):
    """Append-only лог. Никогда не кидает исключение наружу."""
    try:
        if result is None:
            result_json: Any = {}
        elif isinstance(result, (dict, list)):
            result_json = result
        else:
            result_json = {"value": str(result)}
        await db_insert("agent_actions", {
            "tool_name": tool_name,
            "params": params,
            "mode": mode,
            "status": status,
            "result": result_json,
            "error": error or "",
            "pending_id": pending_id,
        })
    except Exception as e:
        print(f"[agent.log_action] failed: {e}")


async def enqueue_pending(
    tool_name: str,
    params: dict,
    reason: str,
    preview: Optional[dict] = None,
) -> dict:
    """Положить действие в очередь на подтверждение."""
    expires = (datetime.utcnow() + timedelta(minutes=PENDING_TTL_MINUTES)).isoformat()
    row = await db_insert("pending_actions", {
        "tool_name": tool_name,
        "params": params,
        "status": "pending",
        "reason": reason,
        "preview": preview or {},
        "expires_at": expires,
    })
    if isinstance(row, list) and row:
        row = row[0]
    return row or {}


# ─────────────────────────────────────────────────────────────
#  Tool handlers
#  Каждая функция получает dict params и возвращает dict result.
#  HTTPException бросается при ошибках валидации.
# ─────────────────────────────────────────────────────────────
def _req(params: dict, key: str):
    v = params.get(key)
    if v is None or v == "":
        raise HTTPException(status_code=400, detail=f"Missing required param: {key}")
    return v


# ── Диалоги и сообщения ──
async def tool_find_conversation(p: dict) -> dict:
    q = str(_req(p, "search")).strip()
    limit = int(p.get("limit", 5))
    # Ищем по Instagram username / имени клиента
    or_filter = (
        f"instagram_username.ilike.*{q}*,"
        f"client_name.ilike.*{q}*,"
        f"last_message_text.ilike.*{q}*"
    )
    rows = await db_select(
        "conversations",
        columns="id,client_name,instagram_username,funnel,status,last_message_text,last_message_at,client_id",
        or_filter=or_filter,
        order="last_message_at.desc",
        limit=limit,
    )
    return {"conversations": rows}


async def tool_get_conversation_messages(p: dict) -> dict:
    cid = int(_req(p, "conversation_id"))
    limit = int(p.get("limit", 30))
    rows = await db_select(
        "messages",
        columns="id,direction,message_type,content,media_url,created_at",
        filters={"conversation_id": cid},
        order="created_at.desc",
        limit=limit,
    )
    return {"conversation_id": cid, "messages": list(reversed(rows or []))}


async def tool_send_message_to_client(p: dict) -> dict:
    cid = int(_req(p, "conversation_id"))
    text = str(_req(p, "text"))
    conv = await db_select("conversations", filters={"id": cid}, single=True)
    recipient = conv.get("instagram_user_id") or ""
    if not recipient:
        raise HTTPException(status_code=400, detail="Conversation has no instagram_user_id")
    result = await send_instagram_message(recipient, text, cid)
    await db_update("conversations", {
        "last_message_text": text[:200],
        "last_message_at": datetime.utcnow().isoformat(),
        "last_message_dir": "out",
        "updated_at": datetime.utcnow().isoformat(),
    }, {"id": cid})
    return {"ok": True, "ig_result": result}


async def tool_update_conversation(p: dict) -> dict:
    cid = int(_req(p, "conversation_id"))
    patch: dict = {"updated_at": datetime.utcnow().isoformat()}
    for k in ("funnel", "status", "assigned_to"):
        if k in p and p[k] is not None:
            patch[k] = p[k]
    # Теги — add/remove (нужно прочитать текущие)
    add = p.get("tags_add") or []
    rem = p.get("tags_remove") or []
    if add or rem:
        cur = await db_select("conversations", columns="tags", filters={"id": cid}, maybe_single=True)
        tags = list((cur or {}).get("tags") or [])
        for t in add:
            if t not in tags:
                tags.append(t)
        tags = [t for t in tags if t not in rem]
        patch["tags"] = tags
    result = await db_update("conversations", patch, {"id": cid})
    return {"ok": True, "conversation": result[0] if isinstance(result, list) and result else result}


# ── Клиенты ──
async def tool_find_or_create_client(p: dict) -> dict:
    name = str(_req(p, "name")).strip()
    phone = (p.get("phone") or "").strip()
    ig_id = (p.get("instagram_user_id") or "").strip()

    found = None
    if phone:
        r = await db_select("clients", filters={"phone": phone}, maybe_single=True)
        found = r
    if not found and ig_id:
        r = await db_select("clients", filters={"instagram_user_id": ig_id}, maybe_single=True)
        found = r

    if found:
        return {"created": False, "client": found}

    payload = {
        "name": name,
        "surname": p.get("surname", ""),
        "phone": phone,
        "instagram_username": p.get("instagram_username", ""),
        "instagram_user_id": ig_id,
        "city": p.get("city", ""),
        "np_branch": p.get("np_branch", ""),
    }
    row = await db_insert("clients", payload)
    if isinstance(row, list) and row:
        row = row[0]
    return {"created": True, "client": row}


async def tool_update_client_card(p: dict) -> dict:
    cid = int(_req(p, "client_id"))
    patch: dict = {"updated_at": datetime.utcnow().isoformat()}
    for k in ("phone", "city", "np_branch", "email", "notes", "name", "surname"):
        if p.get(k) is not None and p.get(k) != "":
            patch[k] = p[k]
    add = p.get("tags_add") or []
    if add:
        cur = await db_select("clients", columns="tags", filters={"id": cid}, maybe_single=True)
        tags = list((cur or {}).get("tags") or [])
        for t in add:
            if t not in tags:
                tags.append(t)
        patch["tags"] = tags
    result = await db_update("clients", patch, {"id": cid})
    return {"ok": True, "client": result[0] if isinstance(result, list) and result else result}


# ── Товары ──
async def tool_search_products(p: dict) -> dict:
    filters: dict = {"is_active": "true"}
    if p.get("category"):
        filters["category"] = p["category"]
    if p.get("gender"):
        filters["gender"] = p["gender"]
    or_filter = ""
    if p.get("search"):
        q = p["search"]
        or_filter = f"name.ilike.*{q}*,sku.ilike.*{q}*,category.ilike.*{q}*"
    rows = await db_select(
        "products",
        columns="id,name,sku,price,stock,sizes,stock_by_size,category,image_url",
        filters=filters,
        or_filter=or_filter,
        order="stock.desc",
        limit=20,
    )
    # Пост-фильтрация по размеру + in_stock_only
    size = p.get("size")
    in_stock_only = p.get("in_stock_only", True)
    out = []
    for r in rows or []:
        if in_stock_only and int(r.get("stock") or 0) <= 0:
            continue
        if size is not None:
            sbs = r.get("stock_by_size") or {}
            if isinstance(sbs, dict) and sbs:
                if int(sbs.get(str(size), 0) or 0) <= 0:
                    continue
            else:
                sizes = r.get("sizes") or []
                if isinstance(sizes, list) and int(size) not in sizes and str(size) not in sizes:
                    continue
        out.append(r)
    return {"products": out}


async def tool_get_product_availability(p: dict) -> dict:
    pid = int(_req(p, "product_id"))
    row = await db_select("products", filters={"id": pid}, single=True)
    return {"product": row}


# ── Заказы ──
async def tool_create_order(p: dict) -> dict:
    client_id = int(_req(p, "client_id"))
    product_id = int(_req(p, "product_id"))
    size = str(_req(p, "size"))
    quantity = int(p.get("quantity", 1))
    prod = await db_select("products", filters={"id": product_id}, single=True)
    price = float(p.get("price") or prod.get("price") or 0)
    total = price * quantity
    payload = {
        "client_id": client_id,
        "product_id": product_id,
        "product_name": prod.get("name", ""),
        "conversation_id": p.get("conversation_id"),
        "size": size,
        "quantity": quantity,
        "price": price,
        "total": total,
        "status": "new",
        "order_type": p.get("order_type", "order"),
        "payment_method": p.get("payment_method", ""),
        "notes": p.get("notes", ""),
    }
    # Списание остатков (тот же паттерн что в index.py create_order)
    try:
        sbs = prod.get("stock_by_size") or {}
        if not isinstance(sbs, dict):
            sbs = {}
        key = str(size)
        cur = int(sbs.get(key, 0) or 0)
        sbs[key] = max(0, cur - quantity)
        stock_total = sum(int(v or 0) for v in sbs.values())
        await db_update("products", {"stock_by_size": sbs, "stock": stock_total}, {"id": product_id})
    except Exception as e:
        print(f"[agent.create_order] stock decrement failed: {e}")
    row = await db_insert("orders", payload)
    if isinstance(row, list) and row:
        row = row[0]
    return {"ok": True, "order": row}


async def tool_update_order_status(p: dict) -> dict:
    oid = int(_req(p, "order_id"))
    ns = str(_req(p, "new_status"))
    allowed = {"new", "confirmed", "paid", "shipped", "delivered", "cancelled", "refunded"}
    if ns not in allowed:
        raise HTTPException(status_code=400, detail=f"Invalid status: {ns}")
    patch = {
        "status": ns,
        "updated_at": datetime.utcnow().isoformat(),
    }
    if p.get("reason"):
        cur = await db_select("orders", columns="notes", filters={"id": oid}, maybe_single=True)
        old_notes = (cur or {}).get("notes") or ""
        stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        patch["notes"] = (old_notes + f"\n[{stamp}] agent→{ns}: {p['reason']}").strip()
    result = await db_update("orders", patch, {"id": oid})
    return {"ok": True, "order": result[0] if isinstance(result, list) and result else result}


async def tool_update_order_details(p: dict) -> dict:
    oid = int(_req(p, "order_id"))
    patch: dict = {"updated_at": datetime.utcnow().isoformat()}
    for k in ("size", "quantity", "price", "notes", "payment_method"):
        if p.get(k) is not None and p.get(k) != "":
            patch[k] = p[k]
    if "quantity" in patch and "price" in patch:
        patch["total"] = float(patch["price"]) * int(patch["quantity"])
    result = await db_update("orders", patch, {"id": oid})
    return {"ok": True, "order": result[0] if isinstance(result, list) and result else result}


async def tool_notify_order_status(p: dict) -> dict:
    oid = int(_req(p, "order_id"))
    order = await db_select("orders", filters={"id": oid}, single=True)
    cid = order.get("conversation_id")
    status = order.get("status", "")
    templates = {
        "confirmed":  "Дякуємо! Ваше замовлення №{oid} підтверджено. Очікуємо оплату.",
        "paid":       "Оплата отримана ✓ Замовлення №{oid} готуємо до відправки.",
        "shipped":    "Замовлення №{oid} відправлено. ТТН: {ttn}",
        "delivered":  "Замовлення №{oid} доставлено. Дякуємо, що обрали IS EASY!",
        "cancelled":  "Замовлення №{oid} скасовано. Якщо це помилка — напишіть нам.",
    }
    tpl = templates.get(status)
    if not tpl:
        return {"ok": False, "reason": f"no template for status {status}"}
    text = tpl.format(oid=oid, ttn=order.get("ttn", "—"))
    if not cid:
        return {"ok": False, "reason": "order has no conversation_id"}
    conv = await db_select("conversations", filters={"id": cid}, single=True)
    recipient = conv.get("instagram_user_id") or ""
    if recipient:
        await send_instagram_message(recipient, text, cid)
    return {"ok": True, "sent_text": text}


# ── Платежи ──
async def tool_list_unmatched_payments(p: dict) -> dict:
    limit = int(p.get("limit", 20))
    rows = await db_select(
        "payments",
        columns="id,source,payer_name,amount,currency,payment_date",
        filters={"is_matched": "false"},
        order="payment_date.desc",
        limit=limit,
    )
    return {"payments": rows}


async def tool_suggest_payment_match(p: dict) -> dict:
    pid = int(_req(p, "payment_id"))
    pay = await db_select("payments", filters={"id": pid}, single=True)
    amount = float(pay.get("amount") or 0)
    # Кандидаты — неоплаченные заказы с близкой суммой за последние 14 дней
    two_weeks_ago = (datetime.utcnow() - timedelta(days=14)).isoformat()
    orders = await db_select(
        "orders",
        columns="id,client,client_id,total,paid,status,created_at",
        gt={"created_at": two_weeks_ago},
        order="created_at.desc",
        limit=100,
    )
    candidates = []
    payer = (pay.get("payer_name") or "").lower()
    for o in orders or []:
        if o.get("status") in ("cancelled", "refunded", "delivered"):
            continue
        total = float(o.get("total") or 0)
        paid = float(o.get("paid") or 0)
        diff = abs((total - paid) - amount)
        score = 0.0
        if diff < 0.01:
            score += 0.7
        elif diff < 1:
            score += 0.5
        elif diff / max(total, 1) < 0.05:
            score += 0.3
        cname = (o.get("client") or "").lower()
        if cname and payer and (cname in payer or payer in cname):
            score += 0.3
        if score >= 0.3:
            candidates.append({
                "order_id": o["id"],
                "client": o.get("client"),
                "total": total,
                "paid": paid,
                "score": round(score, 2),
            })
    candidates.sort(key=lambda x: -x["score"])
    return {"payment": pay, "candidates": candidates[:5]}


async def tool_match_payment_to_order(p: dict) -> dict:
    pid = int(_req(p, "payment_id"))
    oid = int(_req(p, "order_id"))
    pay = await db_select("payments", filters={"id": pid}, single=True)
    order = await db_select("orders", filters={"id": oid}, single=True)
    amount = float(pay.get("amount") or 0)
    new_paid = float(order.get("paid") or 0) + amount
    total = float(order.get("total") or 0)
    order_patch: dict = {
        "paid": new_paid,
        "updated_at": datetime.utcnow().isoformat(),
    }
    if new_paid >= total > 0 and order.get("status") in ("new", "confirmed"):
        order_patch["status"] = "paid"
    await db_update("orders", order_patch, {"id": oid})
    await db_update("payments", {
        "is_matched": True,
        "matched_order_id": oid,
        "matched_conversation_id": order.get("conversation_id"),
    }, {"id": pid})
    return {"ok": True, "order_paid": new_paid, "order_total": total}


# ── Нова Пошта ──
async def _np_get(path: str, params: dict | None = None) -> dict:
    """Internal loopback call to own backend for NP endpoints."""
    base = os.getenv("BACKEND_INTERNAL_URL", "") or ""
    if not base:
        raise HTTPException(status_code=503, detail="BACKEND_INTERNAL_URL not set")
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{base}{path}", params=params or {})
    return r.json()


async def _np_post(path: str, body: dict) -> dict:
    base = os.getenv("BACKEND_INTERNAL_URL", "") or ""
    if not base:
        raise HTTPException(status_code=503, detail="BACKEND_INTERNAL_URL not set")
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(f"{base}{path}", json=body)
    return r.json()


async def tool_np_resolve_city_warehouse(p: dict) -> dict:
    city_text = str(_req(p, "city_text")).strip()
    cities = await _np_get("/api/nova-poshta/cities", {"q": city_text, "limit": 5})
    city_ref = None
    city_name = None
    if isinstance(cities, dict) and cities.get("data"):
        first = cities["data"][0]
        city_ref = first.get("Ref") or first.get("ref")
        city_name = first.get("Description") or first.get("name")
    elif isinstance(cities, list) and cities:
        first = cities[0]
        city_ref = first.get("Ref") or first.get("ref")
        city_name = first.get("Description") or first.get("name")
    warehouse_ref = None
    warehouse_name = None
    if city_ref and p.get("warehouse_text"):
        whs = await _np_get("/api/nova-poshta/warehouses", {"city_ref": city_ref, "q": p["warehouse_text"], "limit": 5})
        whs_data = whs.get("data") if isinstance(whs, dict) else whs
        if whs_data:
            first = whs_data[0]
            warehouse_ref = first.get("Ref") or first.get("ref")
            warehouse_name = first.get("Description") or first.get("name")
    return {
        "city_ref": city_ref,
        "city_name": city_name,
        "warehouse_ref": warehouse_ref,
        "warehouse_name": warehouse_name,
    }


async def tool_np_estimate_delivery_cost(p: dict) -> dict:
    return await _np_post("/api/nova-poshta/estimate-cost", {
        "city_ref": _req(p, "city_ref"),
        "weight": float(p.get("weight_kg") or 1),
        "declared_value": float(_req(p, "declared_value")),
    })


async def tool_np_create_ttn(p: dict) -> dict:
    return await _np_post("/api/nova-poshta/create-ttn", {
        "order_id": int(_req(p, "order_id")),
        "weight": float(p.get("weight_kg") or 1),
        "payer": p.get("payer", "recipient"),
    })


async def tool_np_track_ttn(p: dict) -> dict:
    return await _np_post("/api/nova-poshta/track", {"ttn": _req(p, "ttn")})


# ── Задачи ──
async def tool_create_task(p: dict) -> dict:
    payload = {
        "title": str(_req(p, "title")),
        "description": p.get("description", ""),
        "assignee_id": p.get("assignee_id"),
        "order_id": p.get("order_id"),
        "deadline": p.get("deadline"),
        "source": "agent",
        "status": "open",
    }
    row = await db_insert("tasks", payload)
    if isinstance(row, list) and row:
        row = row[0]
    return {"ok": True, "task": row}


# ── Аналитика ──
async def tool_get_stats(p: dict) -> dict:
    period = p.get("period", "today")
    now = datetime.utcnow()
    if period == "today":
        since = now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "yesterday":
        since = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "week":
        since = now - timedelta(days=7)
    else:
        since = now - timedelta(days=30)
    orders = await db_select(
        "orders",
        columns="id,status,total,paid,created_at",
        gt={"created_at": since.isoformat()},
        limit=500,
    )
    rows = orders or []
    total_cnt = len(rows)
    revenue = sum(float(r.get("total") or 0) for r in rows if r.get("status") not in ("cancelled", "refunded"))
    paid_cnt = sum(1 for r in rows if r.get("status") in ("paid", "shipped", "delivered"))
    return {
        "period": period,
        "orders_total": total_cnt,
        "orders_paid": paid_cnt,
        "revenue_uah": round(revenue, 2),
    }


# ── Автоматизации ──
async def tool_propose_automation(p: dict) -> dict:
    payload = {
        "name": str(_req(p, "name")),
        "trigger_type": str(_req(p, "trigger_type")),
        "trigger_text": p.get("trigger_text", ""),
        "actions": p.get("actions") or [],
        "is_active": False,  # ВСЕГДА false — включает владелец руками
    }
    row = await db_insert("automations", payload)
    if isinstance(row, list) and row:
        row = row[0]
    return {"ok": True, "automation": row, "note": "Created with is_active=false"}


# ─────────────────────────────────────────────────────────────
#  Dispatch table
# ─────────────────────────────────────────────────────────────
TOOL_HANDLERS: dict[str, Callable[[dict], Any]] = {
    "find_conversation":          tool_find_conversation,
    "get_conversation_messages":  tool_get_conversation_messages,
    "send_message_to_client":     tool_send_message_to_client,
    "update_conversation":        tool_update_conversation,
    "find_or_create_client":      tool_find_or_create_client,
    "update_client_card":         tool_update_client_card,
    "search_products":            tool_search_products,
    "get_product_availability":   tool_get_product_availability,
    "create_order":               tool_create_order,
    "update_order_status":        tool_update_order_status,
    "update_order_details":       tool_update_order_details,
    "notify_order_status":        tool_notify_order_status,
    "list_unmatched_payments":    tool_list_unmatched_payments,
    "suggest_payment_match":      tool_suggest_payment_match,
    "match_payment_to_order":     tool_match_payment_to_order,
    "np_resolve_city_warehouse":  tool_np_resolve_city_warehouse,
    "np_estimate_delivery_cost":  tool_np_estimate_delivery_cost,
    "np_create_ttn":              tool_np_create_ttn,
    "np_track_ttn":               tool_np_track_ttn,
    "create_task":                tool_create_task,
    "get_stats":                  tool_get_stats,
    "propose_automation":         tool_propose_automation,
}


def _preview_for(tool_name: str, params: dict) -> dict:
    """Краткое превью действия для UI очереди подтверждений."""
    p = params
    if tool_name == "send_message_to_client":
        return {"to_conversation": p.get("conversation_id"), "text": p.get("text", "")[:300]}
    if tool_name == "create_order":
        return {
            "client_id": p.get("client_id"),
            "product_id": p.get("product_id"),
            "size": p.get("size"),
            "quantity": p.get("quantity", 1),
            "price": p.get("price"),
        }
    if tool_name == "update_order_status":
        return {"order_id": p.get("order_id"), "new_status": p.get("new_status")}
    if tool_name == "np_create_ttn":
        return {"order_id": p.get("order_id"), "weight_kg": p.get("weight_kg", 1)}
    if tool_name == "match_payment_to_order":
        return {"payment_id": p.get("payment_id"), "order_id": p.get("order_id")}
    return {}


# ─────────────────────────────────────────────────────────────
#  Routes
# ─────────────────────────────────────────────────────────────

@router.get("/tools")
async def list_tools(authorization: Optional[str] = Header(None)):
    _check_auth(authorization)
    return {
        "tools": sorted(TOOL_HANDLERS.keys()),
        "modes": TOOL_MODES,
        "thresholds": {
            "order_auto_max_uah": HYBRID_ORDER_AUTO_MAX_UAH,
            "order_auto_max_qty": HYBRID_ORDER_AUTO_MAX_QTY,
            "message_auto_max_len": HYBRID_MSG_AUTO_MAX_LEN,
            "payment_auto_min_conf": HYBRID_PAYMENT_AUTO_MIN_CONF,
        },
    }


@router.post("/execute")
async def execute_tool(request: Request, authorization: Optional[str] = Header(None)):
    """
    Главный вход для Claude.
    Тело: { "tool": "<name>", "params": { ... } }
    Ответ:
      - при mode=auto:    { "status": "executed", "result": ... }
      - при mode=confirm: { "status": "pending",  "pending_id": N, "reason": ... }
    """
    _check_auth(authorization)
    body = await request.json()
    tool_name = body.get("tool") or body.get("name") or ""
    params = body.get("params") or body.get("input") or {}

    if tool_name not in TOOL_HANDLERS:
        raise HTTPException(status_code=404, detail=f"Unknown tool: {tool_name}")

    mode, reason = resolve_mode(tool_name, params)

    if mode == "confirm":
        pending = await enqueue_pending(
            tool_name, params,
            reason=reason or "always-confirm tool",
            preview=_preview_for(tool_name, params),
        )
        await log_action(tool_name, params, mode, "pending", pending_id=pending.get("id"))
        return {
            "status": "pending",
            "pending_id": pending.get("id"),
            "reason": reason or "always-confirm tool",
            "message": "Действие поставлено в очередь на подтверждение менеджером.",
        }

    # auto
    try:
        result = await TOOL_HANDLERS[tool_name](params)
        await log_action(tool_name, params, mode, "executed", result=result)
        return {"status": "executed", "result": result}
    except HTTPException:
        raise
    except Exception as e:
        await log_action(tool_name, params, mode, "failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"Tool failed: {e}")


@router.get("/pending")
async def list_pending(
    status: str = "pending",
    limit: int = 50,
    authorization: Optional[str] = Header(None),
):
    _check_auth(authorization)
    rows = await db_select(
        "pending_actions",
        filters={"status": status},
        order="created_at.desc",
        limit=limit,
    )
    return {"pending": rows}


@router.post("/pending/{pending_id}/approve")
async def approve_pending(pending_id: int, request: Request, authorization: Optional[str] = Header(None)):
    _check_auth(authorization)
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    approver = body.get("approved_by") or "manager"
    row = await db_select("pending_actions", filters={"id": pending_id}, single=True)
    if row.get("status") != "pending":
        raise HTTPException(status_code=409, detail=f"Already {row.get('status')}")
    tool_name = row["tool_name"]
    params = row.get("params") or {}
    # Фактическое выполнение
    try:
        result = await TOOL_HANDLERS[tool_name](params)
    except Exception as e:
        await db_update("pending_actions", {
            "status": "rejected",
            "approved_by": approver,
            "approved_at": datetime.utcnow().isoformat(),
        }, {"id": pending_id})
        await log_action(tool_name, params, "confirm", "failed", error=str(e), pending_id=pending_id)
        raise HTTPException(status_code=500, detail=f"Tool failed on approve: {e}")
    now = datetime.utcnow().isoformat()
    await db_update("pending_actions", {
        "status": "executed",
        "approved_by": approver,
        "approved_at": now,
        "executed_at": now,
    }, {"id": pending_id})
    await log_action(tool_name, params, "confirm", "executed", result=result, pending_id=pending_id)
    return {"status": "executed", "result": result}


@router.post("/pending/{pending_id}/reject")
async def reject_pending(pending_id: int, request: Request, authorization: Optional[str] = Header(None)):
    _check_auth(authorization)
    body = {}
    try:
        body = await request.json()
    except Exception:
        pass
    approver = body.get("approved_by") or "manager"
    row = await db_select("pending_actions", filters={"id": pending_id}, single=True)
    if row.get("status") != "pending":
        raise HTTPException(status_code=409, detail=f"Already {row.get('status')}")
    await db_update("pending_actions", {
        "status": "rejected",
        "approved_by": approver,
        "approved_at": datetime.utcnow().isoformat(),
    }, {"id": pending_id})
    await log_action(
        row["tool_name"], row.get("params") or {}, "confirm", "rejected",
        pending_id=pending_id, error="rejected by manager",
    )
    return {"status": "rejected"}


@router.get("/actions")
async def list_actions(limit: int = 50, authorization: Optional[str] = Header(None)):
    _check_auth(authorization)
    rows = await db_select(
        "agent_actions",
        order="created_at.desc",
        limit=limit,
    )
    return {"actions": rows}


# ─────────────────────────────────────────────────────────────
#  /chat — server-side Claude Opus tool loop
# ─────────────────────────────────────────────────────────────
_AGENT_DIR = Path(__file__).resolve().parent
_TOOLS_CACHE: Optional[list] = None
_SYSTEM_PROMPT_CACHE: Optional[str] = None

_FALLBACK_SYSTEM_PROMPT = (
    "Ты — AI-менеджер бренда обуви IS EASY. Работаешь внутри CRM и помогаешь "
    "команде обрабатывать клиентов. Пиши кратко (2–3 предложения), по-украински, "
    "без эмодзи (кроме 🙏 в конце благодарности). Используй инструменты вместо "
    "догадок. Если не уверен — оставляй задачу через create_task. Финальный ответ "
    "давай менеджеру (не клиенту) в формате отчёта."
)


def _load_agent_tools() -> list:
    global _TOOLS_CACHE
    if _TOOLS_CACHE is not None:
        return _TOOLS_CACHE
    path = _AGENT_DIR / "agent_tools.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and "tools" in data:
            data = data["tools"]
        if not isinstance(data, list):
            raise ValueError("agent_tools.json must be a list")
        _TOOLS_CACHE = data
    except Exception as e:
        print(f"[agent._load_agent_tools] fallback (empty): {e}")
        _TOOLS_CACHE = []
    return _TOOLS_CACHE


def _load_system_prompt() -> str:
    global _SYSTEM_PROMPT_CACHE
    if _SYSTEM_PROMPT_CACHE is not None:
        return _SYSTEM_PROMPT_CACHE
    path = _AGENT_DIR / "agent_system_prompt.md"
    try:
        _SYSTEM_PROMPT_CACHE = path.read_text(encoding="utf-8")
    except Exception as e:
        print(f"[agent._load_system_prompt] fallback: {e}")
        _SYSTEM_PROMPT_CACHE = _FALLBACK_SYSTEM_PROMPT
    return _SYSTEM_PROMPT_CACHE


async def _run_tool_internal(tool_name: str, params: dict) -> dict:
    """
    Общая логика выполнения инструмента (используется /execute и /chat).
    Возвращает {status, result|pending_id|reason, message?, mode}.
    Не бросает HTTPException — упаковывает ошибки в dict.
    """
    if tool_name not in TOOL_HANDLERS:
        return {"status": "error", "error": f"Unknown tool: {tool_name}"}

    mode, reason = resolve_mode(tool_name, params)

    if mode == "confirm":
        try:
            pending = await enqueue_pending(
                tool_name, params,
                reason=reason or "always-confirm tool",
                preview=_preview_for(tool_name, params),
            )
            await log_action(tool_name, params, mode, "pending", pending_id=pending.get("id"))
            return {
                "status": "pending",
                "pending_id": pending.get("id"),
                "reason": reason or "always-confirm tool",
                "message": "Действие поставлено в очередь на подтверждение менеджером.",
                "mode": mode,
            }
        except Exception as e:
            await log_action(tool_name, params, mode, "failed", error=str(e))
            return {"status": "error", "error": f"Enqueue failed: {e}", "mode": mode}

    try:
        result = await TOOL_HANDLERS[tool_name](params)
        await log_action(tool_name, params, mode, "executed", result=result)
        return {"status": "executed", "result": result, "mode": mode}
    except HTTPException as e:
        await log_action(tool_name, params, mode, "failed", error=str(e.detail))
        return {"status": "error", "error": str(e.detail), "mode": mode}
    except Exception as e:
        await log_action(tool_name, params, mode, "failed", error=str(e))
        return {"status": "error", "error": f"Tool failed: {e}", "mode": mode}


@router.post("/chat")
async def agent_chat(request: Request, authorization: Optional[str] = Header(None)):
    """
    Полный цикл общения с Claude Opus: сервер сам гоняет tool loop.
    Тело: { "prompt": str, "model"?: str, "max_turns"?: int }
    Ответ: { "final_text": str, "trace": [...], "turns_used": int, "usage": {...} }
    """
    _check_auth(authorization)

    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not anthropic_key:
        raise HTTPException(
            status_code=503,
            detail="ANTHROPIC_API_KEY is not configured on the server",
        )

    try:
        from anthropic import AsyncAnthropic
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="anthropic package not installed on backend",
        )

    body = await request.json()
    prompt = (body.get("prompt") or body.get("message") or "").strip()
    if not prompt:
        raise HTTPException(status_code=400, detail="Empty prompt")

    model = body.get("model") or os.getenv("CLAUDE_MODEL", "claude-opus-4-6")
    max_turns = int(body.get("max_turns") or os.getenv("MAX_AGENT_TURNS", "8"))
    max_turns = max(1, min(max_turns, 16))

    tools = _load_agent_tools()
    system_prompt = _load_system_prompt()

    client = AsyncAnthropic(api_key=anthropic_key)
    messages: list[dict] = [{"role": "user", "content": prompt}]
    trace: list[dict] = []
    final_text = ""
    total_in = 0
    total_out = 0
    turns_used = 0

    for turn in range(max_turns):
        turns_used = turn + 1
        try:
            resp = await client.messages.create(
                model=model,
                max_tokens=4096,
                system=system_prompt,
                tools=tools,
                messages=messages,
            )
        except Exception as e:
            trace.append({"type": "error", "error": f"Claude API: {e}"})
            raise HTTPException(status_code=502, detail=f"Claude API error: {e}")

        try:
            total_in += resp.usage.input_tokens or 0
            total_out += resp.usage.output_tokens or 0
        except Exception:
            pass

        assistant_blocks = resp.content or []
        tool_uses = [b for b in assistant_blocks if getattr(b, "type", None) == "tool_use"]
        text_blocks = [b for b in assistant_blocks if getattr(b, "type", None) == "text"]

        for tb in text_blocks:
            txt = getattr(tb, "text", "") or ""
            if txt:
                trace.append({"type": "text", "text": txt})
                final_text = txt  # последний текстовый блок = финальный

        if not tool_uses:
            break

        # Добавляем assistant turn с оригинальными блоками
        serialized_assistant = []
        for b in assistant_blocks:
            btype = getattr(b, "type", None)
            if btype == "text":
                serialized_assistant.append({"type": "text", "text": getattr(b, "text", "")})
            elif btype == "tool_use":
                serialized_assistant.append({
                    "type": "tool_use",
                    "id": b.id,
                    "name": b.name,
                    "input": b.input,
                })
        messages.append({"role": "assistant", "content": serialized_assistant})

        # Выполняем все tool_use блоки
        tool_results: list[dict] = []
        for tu in tool_uses:
            tool_name = tu.name
            tool_input = tu.input or {}
            trace.append({
                "type": "tool_use",
                "name": tool_name,
                "input": tool_input,
            })
            result = await _run_tool_internal(tool_name, tool_input)
            trace.append({
                "type": "tool_result",
                "name": tool_name,
                "result": result,
            })
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu.id,
                "content": json.dumps(result, ensure_ascii=False, default=str),
            })

        messages.append({"role": "user", "content": tool_results})

    return {
        "final_text": final_text or "(модель не вернула текст)",
        "trace": trace,
        "turns_used": turns_used,
        "usage": {"input_tokens": total_in, "output_tokens": total_out},
        "model": model,
    }
