"""Deep Up — سيرفر الوسيط بين التطبيق وحساب MEGA.

المسار:
    1) التطبيق يشفّر بريد/كلمة ميجا بمفتاح RSA العام لهذا السيرفر.
    2) السيرفر يفكّها، يفتح جلسة ميجا في الذاكرة فقط (لا تُخزَّن ولا تُسجَّل)، ثم يمسح كلمة المرور.
    3) التطبيق يرسل رابط ملف؛ السيرفر ينزّله (streaming)، ثم يرفعه إلى حساب ميجا.
    4) الخرج: رابط الملف على ميجا + تقدّم لحظي لكل مرحلة.

التشغيل المحلي:
    API_KEYS="me:KEY" WORK_DIR=/tmp/deepup uvicorn main:app --port 9000
"""
from __future__ import annotations

import base64
import glob
import ipaddress
import socket
import hashlib
import json
import os
import random
import re
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import unquote, urlparse, urlunparse, parse_qs, urlencode, urljoin

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import mega_up
import telegram_up

VERSION = "1.3.0"
WORK = os.environ.get("WORK_DIR") or "/tmp/deepup"
os.makedirs(WORK, exist_ok=True)
MAX_MB = int(os.environ.get("MAX_FILE_MB", "2048"))
MAX_JOBS = int(os.environ.get("MAX_JOBS", "40"))
JOB_TTL_MIN = int(os.environ.get("JOB_TTL_MIN", "360"))
ALLOW_PRIVATE = os.environ.get("ALLOW_PRIVATE_HOSTS", "") == "1"
# إعادة المحاولة التلقائية لمرحلة التنزيل (أخطاء الشبكة العابرة و5xx).
# لا نعيد محاولة رفع ميجا لتفادي ملفات مكررة بالحساب.
DOWNLOAD_RETRIES = int(os.environ.get("DOWNLOAD_RETRIES", "2"))      # محاولات إضافية بعد المحاولة الأولى
RETRY_BACKOFF_SEC = float(os.environ.get("RETRY_BACKOFF_SEC", "1.5"))
BATCH_MAX = int(os.environ.get("BATCH_MAX", "10"))                   # أقصى عدد روابط في دفعة واحدة
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

app = FastAPI(title="Deep Up", version=VERSION)


# ─────────────────────────── مفاتيح المستخدمين ───────────────────────────
def _load_keys() -> dict[str, str]:
    raw = os.environ.get("API_KEYS") or os.environ.get("API_KEY") or ""
    out: dict[str, str] = {}
    for i, part in enumerate(p.strip() for p in raw.split(",") if p.strip()):
        name, _, key = part.partition(":")
        if not key:
            name, key = f"user{i + 1}", name
        out[hashlib.sha256(key.encode()).hexdigest()[:16]] = name
    return out


KEYS = _load_keys()


def owner_of(key: str | None) -> str:
    if not key:
        raise HTTPException(401, "مفتاح API مطلوب (X-Api-Key)")
    name = KEYS.get(hashlib.sha256(key.encode()).hexdigest()[:16])
    if not name:
        raise HTTPException(403, "مفتاح API غير معروف")
    return name


def auth(x_api_key: str | None = Header(default=None)) -> str:
    return owner_of(x_api_key)


# ─────────────────────────── أخطاء قابلة لإعادة المحاولة ───────────────────────────
class TransientError(Exception):
    """خطأ عابر أثناء التنزيل (شبكة/5xx/408/429) يستحق إعادة محاولة تلقائية."""


def _retryable(e: Exception) -> bool:
    return isinstance(e, (TransientError, httpx.HTTPError))


# ─────────────────────────── المهام ───────────────────────────
@dataclass
class Job:
    id: str
    owner: str
    url: str
    folder: str = "Deep Up"
    filename: str = ""
    state: str = "queued"          # queued|downloading|uploading|done|error
    stage: str = ""                # نص عربي للعرض
    percent: int = 0
    total: int = 0
    got: int = 0
    link: str = ""
    error: str = ""
    created: float = field(default_factory=time.time)
    seconds: float = 0.0
    sid: str = ""
    attempts: int = 0              # كم محاولة تنزيل استهلكت (1 = من أول مرة)
    smart: bool = False            # «حل ذكي ✨» — استخراج رابط التحميل من صفحة الويب
    resolved_from: str = ""        # من أين حُلّ الرابط (لعرض شفافية في التطبيق)
    tg_sid: str = ""               # جلسة تيليجرام (إن طُلب رفع نسخة للبوت)
    tg: dict[str, Any] = field(default_factory=dict)   # نتيجة الرفع للتيليجرام (state/message_id/error)

    def dict(self) -> dict[str, Any]:
        return {"id": self.id, "filename": self.filename, "state": self.state,
                "stage": self.stage, "percent": self.percent, "total": self.total,
                "got": self.got, "link": self.link, "error": self.error,
                "created": int(self.created), "seconds": self.seconds,
                "url": self.url, "folder": self.folder, "attempts": self.attempts,
                "resolved_from": self.resolved_from, "smart": self.smart,
                "tg": self.tg or {"state": "none"}}


JOBS: dict[str, Job] = {}
LOCK = threading.Lock()


def _get_job(job_id: str, owner: str) -> Job:
    j = JOBS.get(job_id)
    if not j or j.owner != owner:
        raise HTTPException(404, "المهمة غير موجودة")
    return j


def _gc() -> None:
    limit = time.time() - JOB_TTL_MIN * 60
    with LOCK:
        for jid in [k for k, v in JOBS.items() if v.created < limit]:
            j = JOBS.pop(jid, None)
            if j:
                try:
                    p = os.path.join(WORK, j.id + ".bin")
                    if os.path.exists(p):
                        os.remove(p)
                except Exception:
                    pass


# ─────────────────────────── نقاط النهاية ───────────────────────────
@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True, "version": VERSION}


@app.get("/diag")
def diag(owner: str = Depends(auth)) -> dict:
    _gc()
    return {"version": VERSION, "mega_lib": mega_up.info(),
            "rsa": mega_up.rsa_available(), "sessions": mega_up.live_sessions(),
            "keys": len(KEYS), "jobs": len(JOBS), "work_dir": WORK,
            "max_file_mb": MAX_MB, "folder": mega_up.FOLDER,
            "telegram": {"sessions": telegram_up.live_sessions(),
                         "max_mb": telegram_up.MAX_TG_MB,
                         "fake": telegram_up.FAKE}}


class SealedReq(BaseModel):
    sealed: str


class JobReq(BaseModel):
    url: str
    session_id: str = ""
    folder: str = ""
    smart: bool = False
    tg_session_id: str = ""       # إن وُجدت جلسة تيليجرام، تُرسل نسخة من الملف للبوت أيضًا


class BatchReq(BaseModel):
    urls: list[str]
    session_id: str = ""
    folder: str = ""
    smart: bool = False
    tg_session_id: str = ""       # نفس الرمز — كل مهمة في الدفعة تُرفق بتيليجرام


@app.get("/mega/key")
def mega_key(owner: str = Depends(auth)) -> dict:
    if not mega_up.rsa_available():
        raise HTTPException(500, "وحدة التشفير غير متاحة على السيرفر (pycryptodome)")
    return {"alg": "RSA-OAEP-SHA1", "pubkey": mega_up.public_pem()}


@app.post("/mega/session")
def mega_session(req: SealedReq, owner: str = Depends(auth)) -> dict:
    try:
        email, password = mega_up.open_sealed(req.sealed)
    except Exception as e:                                    # noqa: BLE001
        raise HTTPException(400, f"بيانات مشفّرة غير صالحة: {type(e).__name__}")
    if "@" not in email or not password:
        raise HTTPException(400, "بريد أو كلمة مرور غير صالحة")
    try:
        sid = mega_up.create_session(email, password)
    except Exception as e:                                    # noqa: BLE001
        raise HTTPException(401, f"تعذّر تسجيل الدخول إلى ميجا: {type(e).__name__}: {e}"[:300])
    finally:
        password = ""                                     # لا يبقى في الذاكرة أكثر من اللازم
    return {"session_id": sid, "email": mega_up.mask(email),
            "expires_in_sec": mega_up.SESS_TTL}


@app.get("/mega/session/{sid}")
def mega_session_state(sid: str, owner: str = Depends(auth)) -> dict:
    s = mega_up.session(sid)
    return {"alive": s is not None}


@app.delete("/mega/session/{sid}")
def mega_session_drop(sid: str, owner: str = Depends(auth)) -> dict:
    return {"closed": mega_up.drop_session(sid)}


# ─────────────────── تيليجرام: ربط البوت (جلسة مشفّرة في الذاكرة، مثل ميجا) ───────────────────
def _gate_tg_session(owner: str) -> None:
    _ = owner


@app.post("/telegram/session")
def telegram_session_create(req: SealedReq, owner: str = Depends(auth)) -> dict:
    if not mega_up.rsa_available():
        raise HTTPException(500, "وحدة التشفير غير متاحة على السيرفر")
    try:
        token, chat_id = mega_up.open_sealed(req.sealed)
    except Exception as e:                                   # noqa: BLE001
        raise HTTPException(400, "تعذّر فك الظرف المشفّر — حدّث التطبيق وحاول مجددًا") from e
    try:
        sid, info = telegram_up.create_session(token, chat_id)
    except RuntimeError as e:
        raise HTTPException(400, str(e)) from e
    except Exception as e:                                   # noqa: BLE001
        raise HTTPException(400, f"فشل ربط البوت: {type(e).__name__}: {e}"[:200]) from e
    return {"session_id": sid, "bot": info["bot"],
            "chat": telegram_up.mask_chat(chat_id),
            "chat_title": info["chat_title"],
            "expires_in_sec": telegram_up.SESS_TTL}


@app.get("/telegram/session/{sid}")
def telegram_session_state(sid: str, owner: str = Depends(auth)) -> dict:
    return telegram_up.public_info(sid)


@app.delete("/telegram/session/{sid}")
def telegram_session_drop(sid: str, owner: str = Depends(auth)) -> dict:
    return {"closed": telegram_up.drop_session(sid)}


def _host_is_public(host: str) -> bool:
    """True إذا كان المضيف عامًا (ليس عنوانًا محليًا/داخليًا) — يُستخدم في SSRF والحل الذكي.

    يجمّد كل صيغ IPv4 (عشرية/ست عشرية/ثماني مدمجة عبر inet_aton) والمدى
    CGNAT وlink-local (علامة سحابة 169.254.169.254) — ثم يعوّل على is_global.
    ملاحظة صادقة: هجمات «DNS rebinding» (اسم يحل إلى عنوان داخلي) لا يمكن
    إسقاطها كليًا دون الاتصال نفسه — لا تُفتح بها ثغرة لكنها ليست محلولة هنا.
    """
    if ALLOW_PRIVATE:
        return True
    host = (host or "").strip().lower().strip(".")
    if not host:
        return False
    if host.startswith("[") or ":" in host:      # IPv6 — غير مسموح في هذه الأداة
        return False
    if host == "localhost" or host.endswith((".local", ".internal", ".lan", ".home")):
        return False
    ip = None
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        try:
            ip = ipaddress.ip_address(socket.inet_aton(host))   # http://2130706433/ = 127.0.0.1
        except (OSError, ValueError):
            ip = None
    if ip is not None:
        return bool(ip.is_global)
    return True


def _safe_url(url: str) -> None:
    u = urlparse(url)
    if u.scheme not in ("http", "https") or not u.netloc:
        raise HTTPException(400, "الرابط يجب أن يبدأ بـ http أو https")
    host = (u.hostname or "").lower()
    if not _host_is_public(host):
        if host in ("localhost", "0.0.0.0") or host.endswith(".local"):
            raise HTTPException(400, "عنوان محلي غير مسموح")
        raise HTTPException(400, "عنوان شبكة داخلية غير مسموح")


# ─────────────────── حل روابط المشاركة الشائعة إلى روابط تحميل مباشرة ───────────────────
_CTYPE_EXT = {
    "application/pdf": ".pdf",
    "application/zip": ".zip", "application/x-zip-compressed": ".zip",
    "application/x-7z-compressed": ".7z", "application/x-rar-compressed": ".rar",
    "application/gzip": ".gz", "application/x-tar": ".tar",
    "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif", "image/webp": ".webp", "image/svg+xml": ".svg",
    "audio/mpeg": ".mp3", "audio/mp4": ".m4a", "audio/x-m4a": ".m4a",
    "audio/wav": ".wav", "audio/ogg": ".ogg", "audio/flac": ".flac",
    "video/mp4": ".mp4", "video/webm": ".webm", "video/x-matroska": ".mkv", "video/quicktime": ".mov",
    "application/json": ".json", "text/plain": ".txt", "text/csv": ".csv",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.ms-excel": ".xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
}


def _resolve_url(url: str) -> str:
    """يحوّل روابط «صفحة المعاينة» الشائعة إلى روابط تحميل مباشرة.

    مثال Google Drive: رابط /file/.../view هو صفحة عرض وليس الملف — ننتقل
    إلى endpoint التحميل المباشر. Dropbox: dl=0 صفحة معاينة؛ dl=1 يحمّل الملف.
    """
    try:
        u = urlparse(url)
    except Exception:
        return url
    host = (u.hostname or "").lower()
    q = parse_qs(u.query)
    # Google Drive
    m = re.search(r"/file/d/([A-Za-z0-9_-]+)", u.path)
    if m and "google" in host:
        return f"https://drive.usercontent.google.com/download?id={m.group(1)}&export=download&confirm=t"
    if "drive.google" in host and u.path in ("/open", "/uc") and q.get("id"):
        return f"https://drive.usercontent.google.com/download?id={q['id'][0]}&export=download&confirm=t"
    # Dropbox
    if "dropbox.com" in host:
        q["dl"] = ["1"]
        return urlunparse(u._replace(query=urlencode(q, doseq=True)))
    return url


# ─────────────────── روابط mega.nz العامة (المفتاح داخل الرابط) ───────────────────
def _is_mega_link(url: str) -> bool:
    try:
        u = urlparse(url or "")
    except Exception:
        return False
    host = (u.hostname or "").lower()
    return host.endswith("mega.nz") or host.endswith("mega.co.nz")


def _fix_mega_name(name: str) -> str:
    """أسماء ملفات ميجا غالبًا UTF-8 فُكّت كـ latin-1 — نصححها بأفضل جهد."""
    if not name:
        return "mega-file"
    try:
        return _clean(name.encode("latin-1").decode("utf-8"))
    except Exception:
        return _clean(name)


def _mega_public_download(job: Job) -> str:
    """ينزّل ملفًا من رابط mega.nz عام إلى WORK/{id}.bin.

    ميجا تُشفّر الملفات بمفتاح مرفق داخل fragment (#...) — لذا يمكن تنزيلها وفكها
    بالكامل بلا حساب مسجّل. هذا تنزيل فقط — لا شيء يُنشأ في حساب أحد.
    """
    path = os.path.join(WORK, job.id + ".bin")
    job.state = "downloading"
    job.stage = "جارٍ الاتصال بميجا…"
    job.percent = 0
    started = time.time()
    try:
        if mega_up.FAKE:
            client = mega_up._FakeMega()
            info = client.get_public_url_info(job.url)
        else:
            client = mega_up._import_mega()()
            info = client.get_public_url_info(job.url)
    except Exception as e:                                   # noqa: BLE001
        raise TransientError(f"ميجا: {type(e).__name__}: {e}"[:200])
    size = int(info.get("size") or 0)
    if size and size > MAX_MB * 1048576:
        raise RuntimeError(f"الحجم يتجاوز الحد ({MAX_MB} م.ب)")
    job.total = size
    job.filename = _fix_mega_name(info.get("name", ""))
    job.stage = f"جارٍ التنزيل من ميجا ({job.filename})"

    result: dict[str, Any] = {}

    def runner() -> None:
        try:
            result["path"] = str(client.download_url(job.url, WORK, job.id + ".bin"))
        except Exception as e:                               # noqa: BLE001
            result["error"] = e

    tmp_dir = tempfile.gettempdir()
    try:
        pre_existing = set(glob.glob(os.path.join(tmp_dir, "megapy_*")))
    except Exception:
        pre_existing = set()
    t = threading.Thread(target=runner, name="mega-dl-" + job.id, daemon=True)
    t.start()
    # مراقبة الملف المؤقت لتغذية نسبة التقدم (أفضل جهد؛ دقيقة للمهام المفردة)
    while t.is_alive():
        try:
            cands = [c for c in glob.glob(os.path.join(tmp_dir, "megapy_*"))
                     if c not in pre_existing and os.path.getmtime(c) >= started - 1]
            if cands and size:
                got = max(os.path.getsize(c) for c in cands)
                job.got = min(got, size)
                job.percent = int(job.got * 100 / size)
                job.stage = f"تنزيل من ميجا: {job.got // 1048576} من {size // 1048576} م.ب"
        except Exception:
            pass
        time.sleep(0.4)
    t.join()
    if "error" in result:
        e = result["error"]
        msg = f"{type(e).__name__}: {e}".lower()
        if any(s in msg for s in ("retrying", "timed out", "timeout", "connection", "reset", "temporarily")):
            raise TransientError(f"ميجا: {e}"[:200])
        if "mac" in msg:
            raise RuntimeError("تعذّر فك تشفير ملف ميجا — الرابط غير مكتمل أو الملف تالف")
        raise RuntimeError(f"ميجا: {type(e).__name__}: {e}"[:200])
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        raise TransientError("ميجا أرجع ملفًا فارغًا")
    job.total = os.path.getsize(path)
    job.got = job.total
    return path


# ─────────────────── «حل ذكي ✨»: استخراج رابط التنزيل من صفحات الويب ───────────────────
_DL_WORD = re.compile(r"(download|تحميل|تنزيل|مباشر|direct)", re.I)
_FILE_EXT = re.compile(r"\.(zip|rar|7z|tar|gz|bz2|pdf|mp3|mp4|m4a|mkv|avi|mov|webm|flv|apk|exe|msi|doc|docx|xls|xlsx|ppt|pptx|iso|img|torrent)(\?|#|$)", re.I)
_HREF_HINT = re.compile(r"(/download|/dl/|/files?/|export=download|confirm=t|attdownload=1)", re.I)
_BAD_HOST = re.compile(r"(doubleclick\.net|googleadservices|adservice|adnxs\.com|adsystem|advertising|click\.|track|analytics)", re.I)


def _smart_candidates(page_url: str, body: bytes) -> list[str]:
    """يرشّح أقوى روابط التنزيل في صفحة HTML — مرتبة بالقوة، بلا إعلانات أو صفحات دخول."""
    try:
        from bs4 import BeautifulSoup
    except Exception:
        raise RuntimeError("مكتبة التحليل (beautifulsoup4) غير مثبتة على السيرفر — أضِفها إلى requirements.txt.")
    try:
        soup = BeautifulSoup(body, "html.parser")
    except Exception:
        return []
    seen: set[str] = set()
    scored: list[tuple[int, str]] = []

    def add(href: str | None, text: str) -> None:
        if not href:
            return
        href = href.strip()
        if not href or href.startswith(("javascript:", "mailto:", "#", "data:")):
            return
        url = urljoin(page_url, href)
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return
        host = (p.hostname or "").lower()
        if not _host_is_public(host) or _BAD_HOST.search(host):
            return
        score = 0
        if _FILE_EXT.search(url):
            score += 3
        if _HREF_HINT.search(url):
            score += 2
        if _DL_WORD.search(text or ""):
            score += 2
        if "drive.usercontent.google.com" in url or "dl.dropboxusercontent.com" in url:
            score += 4
        if re.search(r"(login|signin|signup|register|auth)", url.lower()):
            score -= 5
        if score >= 3 and url not in seen:
            seen.add(url)
            scored.append((score, url))

    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True)
        attrs = " ".join(str(v) for v in a.attrs.values() if isinstance(v, str))
        add(a["href"], f"{text} {attrs}")
    # <meta http-equiv="refresh" content="0;url=...">
    meta = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
    if meta and meta.get("content"):
        m = re.search(r"url\s*=\s*(.+)$", str(meta["content"]), re.I)
        if m:
            add(m.group(1).strip(), "refresh download")

    scored.sort(key=lambda x: x[0], reverse=True)
    return [u for _, u in scored[:3]]


class _HtmlPage(Exception):
    """الاستجابة صفحة HTML — نحمل جسمها لاستخدامه في «الحل الذكي»."""

    def __init__(self, body: bytes):
        super().__init__("صفحة ويب وليست ملفًا")
        self.body = (body or b"")[:5 << 20]   # أقصى 5 م.ب للتحليل


def _read_capped(resp: Any, limit: int) -> bytes:
    buf = bytearray()
    for chunk in resp.iter_bytes(1 << 16):
        buf += chunk
        if len(buf) >= limit:
            break
    return bytes(buf[:limit])


def _guess_name(url: str, cd: str, ct: str) -> str:
    name = ""
    if cd:
        m = re.search(r"filename\*=UTF-8''([^;]+)", cd, re.I)
        if m:
            name = unquote(m.group(1))
        else:
            m = re.search(r'filename="?([^";]+)"?', cd, re.I)
            if m:
                name = m.group(1)
    if not name:
        name = unquote(os.path.basename(urlparse(url).path)) or "file"
    name = _clean(name)
    if "." not in name:
        ext = _CTYPE_EXT.get((ct or "").split(";")[0].strip().lower())
        if ext:
            name = _clean(name + ext)
    return name


def _clean(name: str) -> str:
    name = name.replace("\\", "/").split("/")[-1]
    name = re.sub(r'[<>:"|?*\x00-\x1f]', "", name).strip()
    return (name or "file")[:180]


def _open_stream(c: httpx.Client, url: str):
    """يفتح بثّ GET مع تتبع التحويلات يدويًا — كل قفزة تُفحص ضد SSRF."""
    hops = 0
    while True:
        u = urlparse(url)
        if u.scheme not in ("http", "https") or not _host_is_public((u.hostname or "").lower()):
            raise RuntimeError("الرابط يحوّل إلى عنوان داخلي غير مسموح")
        req = c.build_request("GET", url)
        r = c.send(req, stream=True)
        if r.status_code in (301, 302, 303, 307, 308):
            loc = r.headers.get("location") or ""
            r.close()
            if not loc:
                raise RuntimeError("إعادة توجيه بلا وجهة")
            url = urljoin(url, loc)
            hops += 1
            if hops > 6:
                raise TransientError("عدد تحويلات كبير — تعذّر التنزيل")
            continue
        return r, url


def _stream_to_file(job: Job, url: str) -> str:
    """يبثّ url إلى WORK/{id}.bin — يرفع _HtmlPage عند جواب HTML."""
    path = os.path.join(WORK, job.id + ".bin")
    job.state = "downloading"
    job.stage = "جارٍ التنزيل من الرابط" if not job.resolved_from else "وُجد رابط تحميل — جارٍ تنزيله"
    job.percent = 0
    job.got = 0
    try:
        with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(30, read=120),
                          headers={"User-Agent": USER_AGENT}) as c:
            r, final_url = _open_stream(c, url)
            try:
                if r.status_code >= 400:
                    if r.status_code in (408, 429) or r.status_code >= 500:
                        raise TransientError(f"الرابط رد {r.status_code}")
                    raise RuntimeError(f"الرابط رد {r.status_code}")
                ctype = (r.headers.get("content-type") or "").lower()
                if "text/html" in ctype:
                    raise _HtmlPage(_read_capped(r, 5 << 20))
                job.filename = job.filename or _guess_name(final_url, r.headers.get("content-disposition", ""),
                                                           r.headers.get("content-type", ""))
                job.total = int(r.headers.get("content-length") or 0)
                if job.total > MAX_MB * 1048576:
                    raise RuntimeError(f"الحجم يتجاوز الحد ({MAX_MB} م.ب)")
                got = 0
                first = True
                with open(path, "wb") as fh:
                    for chunk in r.iter_bytes(1 << 16):
                        if first:
                            head = chunk[:512].lstrip().lower()
                            if head.startswith((b"<!doctype html", b"<html")):
                                raise _HtmlPage(bytes(chunk) + _read_capped(r, 5 << 20))
                            first = False
                        fh.write(chunk)
                        got += len(chunk)
                        job.got = got
                        if MAX_MB and got > MAX_MB * 1048576:
                            raise RuntimeError(f"الحجم تجاوز الحد ({MAX_MB} م.ب)")
                        if job.total:
                            job.percent = int(got * 100 / job.total)
                            job.stage = f"تنزيل {got // 1048576} من {job.total // 1048576} م.ب"
                        else:
                            job.stage = f"تنزيل {got // 1048576} م.ب"
            finally:
                try:
                    r.close()
                except Exception:
                    pass
        if os.path.getsize(path) == 0:
            raise TransientError("الملف المُنزَّل فارغ")
        job.total = os.path.getsize(path)
        return path
    except BaseException:
        # لا نُبقي بقايا ملف تنزيل فاشل على القرص
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass
        raise


def _download(job: Job) -> str:
    """التوجيه: mega.nz → تنزيل مباشر · رابط مباشر → بثّ · صفحة ويب → «حل ذكي» عند التفعيل."""
    if _is_mega_link(job.url):
        return _mega_public_download(job)
    final_url = _resolve_url(job.url)
    if final_url != job.url:
        job.stage = "حُلّ الرابط إلى تحميل مباشر — جارٍ التنزيل"
    try:
        return _stream_to_file(job, final_url)
    except _HtmlPage as page:
        if not job.smart:
            raise RuntimeError("الرابط أرجع صفحة ويب (HTML) وليست ملفًا — فعّل «حل ذكي ✨» للمحاولة تلقائيًا، أو استخدم رابط تحميل مباشر")
        cands = _smart_candidates(final_url, page.body)
        if not cands:
            raise RuntimeError("صفحة ويب بلا رابط تحميل صالح فيها — استخدم رابط تحميل مباشر")
        last_err: Exception | None = None
        for cand in cands:
            job.filename = ""            # اسمح للمرشح الجديد بفرض اسمه
            job.got = 0
            job.percent = 0
            job.resolved_from = job.url
            try:
                return _stream_to_file(job, _resolve_url(cand))
            except _HtmlPage:
                last_err = RuntimeError(f"{cand[:120]} أرجع صفحة ويب أيضًا")
            except Exception as e:       # noqa: BLE001
                last_err = e
        raise RuntimeError(f"لم ينجح أي رابط في الصفحة: {last_err}"[:300])


def _worker(job: Job) -> None:
    t0 = time.time()
    path = ""
    try:
        # تنزيل مع إعادة محاولة تلقائية (أخطاء عابرة فقط) مع تراجع تدريجي
        max_attempts = max(1, DOWNLOAD_RETRIES + 1)
        while True:
            job.attempts += 1
            try:
                path = _download(job)
                break
            except Exception as e:                            # noqa: BLE001
                if job.attempts >= max_attempts or not _retryable(e):
                    raise
                wait = RETRY_BACKOFF_SEC * (2 ** (job.attempts - 1)) + random.uniform(0, 0.5)
                job.stage = f"تعذّر التنزيل — محاولة تلقائية {job.attempts + 1}/{max_attempts} بعد {wait:.0f} ث…"
                time.sleep(wait)
        job.state = "uploading"
        job.percent = 0
        job.stage = "جارٍ الرفع إلى MEGA"
        sess = mega_up.session(job.sid) if job.sid else None
        mega_attempted = sess is not None or mega_up.enabled()
        if sess is None and not mega_attempted and not job.tg_sid:
            raise RuntimeError("انتهت جلسة MEGA — سجّل الدخول من جديد")

        def on_state(**kw: Any) -> None:
            if kw.get("percent") is not None:
                job.percent = int(kw.get("percent") or 0)
            if kw.get("error"):
                job.error = str(kw["error"])

        if sess is not None:
            job.link = mega_up.upload_with(sess["client"], path, job.filename, job.folder, on_state) or ""
        elif mega_up.enabled():
            # وضع بدون جلسة (MEGA_EMAIL/MEGA_PASSWORD): رفع متزامن بنفس توقيع on_state
            job.link = mega_up.upload_file(None, path, job.filename, job.folder, on_state) or ""
        else:
            job.link = ""          # لا جلسة ميجا ولا وضع بيئة — تيليجرام وحده
            job.stage = "تخطّي ميجا (لا جلسة) — تكملة رفع تيليجرام…"
        if not job.link and mega_attempted:
            job.link = mega_up.last_link()
        # ── تيليجرام: وجهة إضافية (اختيارية) ──
        if job.tg_sid:
            tg_sess = telegram_up.session(job.tg_sid)
            if tg_sess is None:
                job.tg = {"state": "failed", "error": "انتهت جلسة تيليجرام — اربط البوت من جديد"}
            else:
                try:
                    job.stage = "جارٍ الرفع إلى تيليجرام…"
                    res = telegram_up.send_document(tg_sess, path, job.filename)
                    job.tg = {"state": "sent", "message_id": res.get("message_id"),
                              "chat_id": res.get("chat_id"), "bot": res.get("bot")}
                except Exception as e:                        # noqa: BLE001
                    job.tg = {"state": "failed",
                              "error": f"{type(e).__name__}: {e}"[:200]}
        job.state = "done"
        job.percent = 100
        if not job.link and job.tg_sid:
            job.stage = ("تم الرفع إلى تيليجرام ✓" if job.tg.get("state") == "sent"
                         else "انتهت المهمة — راجع نتيجة تيليجرام")
        else:
            job.stage = "تم الرفع ✓"
    except Exception as e:                                    # noqa: BLE001
        job.state = "error"
        job.error = f"{type(e).__name__}: {e}"[:300]
        job.stage = "فشل: " + job.error[:80]
    finally:
        job.seconds = round(time.time() - t0, 1)
        try:
            if path and os.path.exists(path):
                os.remove(path)                                # لا نُبقي نسخة على القرص
        except Exception:
            pass


def _make_job(owner: str, url: str, sid: str, folder: str, smart: bool = False,
               tg_sid: str = "") -> Optional[Job]:
    """ينشئ مهمة ويشغّلها في Thread — يعيد None لو بلغنا الحد الأقصى للمهام."""
    folder = (folder or "").strip() or mega_up.FOLDER
    with LOCK:
        if len(JOBS) >= MAX_JOBS:
            return None
        job = Job(id=uuid.uuid4().hex[:12], owner=owner, url=url, sid=sid, folder=folder,
                  smart=bool(smart), tg_sid=(tg_sid or "").strip())
        JOBS[job.id] = job
    threading.Thread(target=_worker, args=(job,), name="deepup-" + job.id, daemon=True).start()
    return job


@app.post("/jobs", status_code=202)
def create_job(req: JobReq, owner: str = Depends(auth)) -> dict:
    _gc()
    _safe_url(req.url.strip())
    job = _make_job(owner, req.url.strip(), req.session_id, req.folder, req.smart, req.tg_session_id)
    if job is None:
        raise HTTPException(429, "عدد المهام كبير — انتظر قليلًا أو احذف مهامًا قديمة")
    return job.dict()


@app.post("/jobs/batch", status_code=202)
def create_batch(req: BatchReq, owner: str = Depends(auth)) -> dict:
    """دفعة روابط: كل رابط يُنشئ مهمة مستقلة (تنزيل ثم رفع إلى ميجا).

    يعيد {created:[...], rejected:[{url, reason}], count:N} — الروابط المرفوضة
    (غير http/https أو عناوين داخلية أو تجاوز الحد) تُتجاهل باقي الدفعة.
    """
    _gc()
    # تنظيف + إزالة التكرار مع الحفاظ على ترتيب إدخال المستخدم
    seen: set[str] = set()
    uniq: list[str] = []
    for u in req.urls or []:
        u = (u or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    if not uniq:
        raise HTTPException(400, "لا توجد روابط صالحة في الدفعة")
    if len(uniq) > BATCH_MAX:
        raise HTTPException(400, f"الحد الأقصى {BATCH_MAX} رابطًا في الدفعة الواحدة")
    created: list[dict] = []
    rejected: list[dict] = []
    for u in uniq:
        try:
            _safe_url(u)
        except HTTPException as e:
            rejected.append({"url": u, "reason": str(e.detail)})
            continue
        job = _make_job(owner, u, req.session_id, req.folder, req.smart, req.tg_session_id)
        if job is None:
            rejected.append({"url": u, "reason": "بلغت الحد الأقصى للمهام المتزامنة"})
            continue
        created.append(job.dict())
    return {"created": created, "rejected": rejected, "count": len(created)}


@app.get("/jobs")
def list_jobs(owner: str = Depends(auth)) -> dict:
    _gc()
    mine = [j for j in JOBS.values() if j.owner == owner]
    mine.sort(key=lambda j: j.created, reverse=True)
    return {"count": len(mine), "jobs": [j.dict() for j in mine]}


@app.get("/jobs/{job_id}")
def get_job(job_id: str, owner: str = Depends(auth)) -> dict:
    return _get_job(job_id, owner).dict()


@app.delete("/jobs/{job_id}")
def delete_job(job_id: str, owner: str = Depends(auth)) -> dict:
    j = _get_job(job_id, owner)
    with LOCK:
        JOBS.pop(job_id, None)
    try:
        p = os.path.join(WORK, job_id + ".bin")
        if os.path.exists(p):
            os.remove(p)
    except Exception:
        pass
    return {"deleted": j.id}


@app.get("/_fake/store")
def fake_store() -> dict:
    """للاستخدام في الاختبار المحلي فقط (DEEPUP_FAKE_MEGA=1)."""
    if not mega_up.FAKE:
        raise HTTPException(404, "غير مفعّل")
    return {"nodes": mega_up.FAKE_STORE}


@app.exception_handler(httpx.HTTPError)
def _http_err(_r: Request, exc: httpx.HTTPError):        # pragma: no cover
    return JSONResponse({"detail": f"خطأ شبكة: {type(exc).__name__}"}, status_code=502)
