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
import hashlib
import json
import os
import random
import re
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.parse import unquote, urlparse, urlunparse, parse_qs, urlencode

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import mega_up

VERSION = "1.1.1"
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

    def dict(self) -> dict[str, Any]:
        return {"id": self.id, "filename": self.filename, "state": self.state,
                "stage": self.stage, "percent": self.percent, "total": self.total,
                "got": self.got, "link": self.link, "error": self.error,
                "created": int(self.created), "seconds": self.seconds,
                "url": self.url, "folder": self.folder, "attempts": self.attempts}


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
            "max_file_mb": MAX_MB, "folder": mega_up.FOLDER}


class SealedReq(BaseModel):
    sealed: str


class JobReq(BaseModel):
    url: str
    session_id: str = ""
    folder: str = ""


class BatchReq(BaseModel):
    urls: list[str]
    session_id: str = ""
    folder: str = ""


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


def _safe_url(url: str) -> None:
    u = urlparse(url)
    if u.scheme not in ("http", "https") or not u.netloc:
        raise HTTPException(400, "الرابط يجب أن يبدأ بـ http أو https")
    host = (u.hostname or "").lower()
    if ALLOW_PRIVATE:
        return
    if host in ("localhost", "0.0.0.0") or host.endswith(".local"):
        raise HTTPException(400, "عنوان محلي غير مسموح")
    if re.fullmatch(r"(127|10|192\.168|169\.254)\.\d+\.\d+\.\d+", host):
        raise HTTPException(400, "عنوان شبكة داخلية غير مسموح")
    if re.fullmatch(r"172\.(1[6-9]|2\d|3[01])\.\d+\.\d+", host):
        raise HTTPException(400, "عنوان شبكة داخلية غير مسموح")
    if host.startswith("[") or ":" in host:
        raise HTTPException(400, "IPv6 غير مسموح")


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


def _download(job: Job) -> str:
    """ينزّل الرابط إلى WORK/{id}.bin ويعيد المسار."""
    path = os.path.join(WORK, job.id + ".bin")
    job.state = "downloading"
    job.stage = "جارٍ التنزيل من الرابط"
    job.percent = 0
    final_url = _resolve_url(job.url)
    if final_url != job.url:
        job.stage = "حُلّ الرابط إلى تحميل مباشر — جارٍ التنزيل"
    try:
        with httpx.Client(follow_redirects=True, timeout=httpx.Timeout(30, read=120),
                          headers={"User-Agent": USER_AGENT}) as c:
            with c.stream("GET", final_url) as r:
                if r.status_code >= 400:
                    if r.status_code in (408, 429) or r.status_code >= 500:
                        raise TransientError(f"الرابط رد {r.status_code}")
                    raise RuntimeError(f"الرابط رد {r.status_code}")
                # روابط الصفحات (Google Drive القديم / Dropbox dl=0 / صفحات معطوبة)
                # تُرجع HTML وليست ملفًا — نرفضها برسالة واضحة بدل رفع ملف تافه
                ctype = (r.headers.get("content-type") or "").lower()
                if "text/html" in ctype:
                    raise RuntimeError("الرابط أرجع صفحة ويب (HTML) وليست ملفًا — استخدم رابط «تحميل مباشر» للملف")
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
                                raise RuntimeError("الرابط أرجع صفحة ويب (HTML) وليست ملفًا — استخدم رابط «تحميل مباشر» للملف")
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
        if sess is None and not mega_up.enabled():
            raise RuntimeError("انتهت جلسة MEGA — سجّل الدخول من جديد")

        def on_state(**kw: Any) -> None:
            if kw.get("percent") is not None:
                job.percent = int(kw.get("percent") or 0)
            if kw.get("error"):
                job.error = str(kw["error"])

        if sess is not None:
            job.link = mega_up.upload_with(sess["client"], path, job.filename, job.folder, on_state) or ""
        else:
            # وضع بدون جلسة (MEGA_EMAIL/MEGA_PASSWORD): رفع متزامن بنفس توقيع on_state
            job.link = mega_up.upload_file(None, path, job.filename, job.folder, on_state) or ""
        if not job.link:
            job.link = mega_up.last_link()
        job.state = "done"
        job.percent = 100
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


def _make_job(owner: str, url: str, sid: str, folder: str) -> Optional[Job]:
    """ينشئ مهمة ويشغّلها في Thread — يعيد None لو بلغنا الحد الأقصى للمهام."""
    folder = (folder or "").strip() or mega_up.FOLDER
    with LOCK:
        if len(JOBS) >= MAX_JOBS:
            return None
        job = Job(id=uuid.uuid4().hex[:12], owner=owner, url=url, sid=sid, folder=folder)
        JOBS[job.id] = job
    threading.Thread(target=_worker, args=(job,), name="deepup-" + job.id, daemon=True).start()
    return job


@app.post("/jobs", status_code=202)
def create_job(req: JobReq, owner: str = Depends(auth)) -> dict:
    _gc()
    _safe_url(req.url.strip())
    job = _make_job(owner, req.url.strip(), req.session_id, req.folder)
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
        job = _make_job(owner, u, req.session_id, req.folder)
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
