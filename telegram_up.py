"""Deep Up — رفع الملف النهائي إلى تيليجرام عبر Bot API (sendDocument).

بيانات البوت (token + chat_id) تُدخل من التطبيق، تُشفَّر RSA-OAEP على الهاتف،
تُفك في ذاكرة السيرفر فقط، وتُحتفظ كجلسة مؤقتة (مثل جلسات ميجا). لا تُكتب
على القرص ولا تظهر في السجلات ولا تُرسل في التسمية (caption).

أوضاع البيئة:
    TELEGRAM_MAX_MB          حد الحجم للإرسال (الافتراضي 50 — سقف Bot API العام)
    TELEGRAM_SESS_TTL_MIN    صلاحية الجلسة بالدقائق (الافتراضي 720 = 12 ساعة)
    DEEPUP_FAKE_TELEGRAM=1   محاكاة كاملة للاختبار المحلي (لا يصل لشبكة)

حد عام: Bot API الرسمي يستقبل حتى 50MB للملف. للملفات الأكبر يلزم
Local Bot API Server على الجهاز نفسه (غير مغطى هنا).
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any

import httpx

MAX_TG_MB = float(os.environ.get("TELEGRAM_MAX_MB", "50"))
SESS_TTL = int(os.environ.get("TELEGRAM_SESS_TTL_MIN", "720")) * 60
API = "https://api.telegram.org"

SESSIONS: dict[str, dict] = {}
_LOCK = threading.Lock()

FAKE = os.environ.get("DEEPUP_FAKE_TELEGRAM", "") == "1"
FAKE_STORE: list[dict] = []                       # سجل رسائل «أُرسلت» في الوضع الوهمي


def mask_chat(cid: str) -> str:
    cid = str(cid or "")
    if len(cid) <= 6:
        return "•" * len(cid)
    return cid[:4] + "•" * (len(cid) - 6) + cid[-2:]


def _api(token: str, method: str, *, json_body: Any = None,
         data: Any = None, files: Any = None, timeout: float = 60) -> dict:
    url = f"{API}/bot{token}/{method}"
    try:
        if files is not None:
            r = httpx.post(url, data=data or {}, files=files,
                           timeout=httpx.Timeout(30, read=180))
        elif json_body is not None:
            r = httpx.post(url, json=json_body, timeout=timeout)
        else:
            r = httpx.get(url, timeout=timeout)
    except httpx.HTTPError as e:
        raise RuntimeError(f"تعذّر الاتصال بتيليجرام: {type(e).__name__}") from e
    try:
        j = r.json()
    except Exception as e:                                # noqa: BLE001
        raise RuntimeError(f"رد تيليجرام غير مفهوم (HTTP {r.status_code})") from e
    if not isinstance(j, dict):
        raise RuntimeError("رد تيليجرام غير متوقع")
    return j


def validate(token: str, chat_id: str) -> dict:
    """يتحقق أن الـtoken يعمل وأن البوت يقدر يصل إلى المحادثة."""
    if FAKE:
        return {"bot": "deepup_test_bot", "chat_title": "محادثة الاختبار", "chat_type": "private"}
    me = _api(token, "getMe")
    if not me.get("ok"):
        desc = (me.get("description") or "").lower()
        if "unauthorized" in desc or "token" in desc:
            raise RuntimeError("رمز البوت غير صالح — تأكد من نسخه من @BotFather كاملًا")
        raise RuntimeError("فشل getMe: " + str(me.get("description") or "سبب غير معروف")[:150])
    chat = _api(token, "getChat", json_body={"chat_id": str(chat_id)})
    if not chat.get("ok"):
        raise RuntimeError(
            "تعذّر الوصول للمحادثة — تأكد أن البوت **عضو** فيها وأن chat_id صحيح "
            f"({str(chat.get('description') or '')[:120]})")
    result = chat.get("result", {})
    title = (result.get("title") or result.get("first_name")
             or result.get("username") or str(chat_id))
    return {"bot": me.get("result", {}).get("username", "bot"),
            "chat_title": str(title)[:80],
            "chat_type": result.get("type", "")}


def create_session(token: str, chat_id: str) -> tuple[str, dict]:
    token = (token or "").strip()
    chat_id = str(chat_id or "").strip()
    if not token or not chat_id:
        raise RuntimeError("الرمز ورقم المحادثة مطلوبان")
    info = validate(token, chat_id)
    with _LOCK:
        sid = base64url()
        SESSIONS[sid] = {"token": token, "chat_id": chat_id, "at": time.time(),
                         "bot": info["bot"], "chat_title": info["chat_title"],
                         "chat_type": info["chat_type"], "uploads": 0}
    return sid, info


def base64url() -> str:
    import base64
    return base64.urlsafe_b64encode(os.urandom(18)).decode().rstrip("=")


def session(sid: str) -> dict | None:
    with _LOCK:
        s = SESSIONS.get(sid)
        if not s:
            return None
        if time.time() - s["at"] > SESS_TTL:
            SESSIONS.pop(sid, None)
            return None
        return s


def drop_session(sid: str) -> bool:
    with _LOCK:
        s = SESSIONS.pop(sid, None)
    return bool(s)


def live_sessions() -> int:
    with _LOCK:
        for k in [k for k, v in SESSIONS.items() if time.time() - v["at"] > SESS_TTL]:
            SESSIONS.pop(k, None)
        return len(SESSIONS)


def public_info(sid: str) -> dict:
    s = session(sid)
    if not s:
        return {"alive": False}
    return {"alive": True, "bot": s.get("bot"), "chat": mask_chat(s.get("chat_id")),
            "chat_title": s.get("chat_title"), "uploads": s.get("uploads", 0)}


def tg_too_big(size: int) -> bool:
    return size > MAX_TG_MB * 1048576


def send_document(sess: dict, path: str, filename: str) -> dict:
    """يرسل الملف كرسالة مستند — يعيد {message_id, chat_id}."""
    size = os.path.getsize(path)
    if tg_too_big(size):
        raise RuntimeError(f"الملف {size // 1048576}MB أكبر من حد تيليجرام ({MAX_TG_MB:.0f}MB)")
    safe_name = filename or "file.bin"
    caption = safe_name[:1024]        # لا نرسل الرابط المصدري أبدًا (قد يحمل مفتاح ميجا)
    if FAKE:
        msg = {"message_id": len(FAKE_STORE) + 1001,
               "chat_id": str(sess["chat_id"]), "bot": sess.get("bot"),
               "file": safe_name, "size": size}
        FAKE_STORE.append(msg)
        sess["uploads"] = sess.get("uploads", 0) + 1
        return msg
    with open(path, "rb") as fh:
        files = {"document": (safe_name, fh, "application/octet-stream")}
        j = _api(sess["token"], "sendDocument",
                 data={"chat_id": str(sess["chat_id"]), "caption": caption},
                 files=files)
    if not j.get("ok"):
        desc = str(j.get("description") or "سبب غير معروف")[:200]
        low = desc.lower()
        if "too big" in low or "file is too" in low:
            desc = f"تيليجرام رفض الملف — أكبر من حده أو المحادثة ممتلئة ({desc})"
        raise RuntimeError("فشل sendDocument: " + desc)
    result = j.get("result", {})
    sess["uploads"] = sess.get("uploads", 0) + 1
    return {"message_id": result.get("message_id"),
            "chat_id": str(result.get("chat", {}).get("id", sess["chat_id"])),
            "bot": sess.get("bot"), "file": safe_name, "size": size}
