"""Deep Up — رفع الملفات إلى حساب MEGA الخاص بالمستخدم.

يُفعَّل فقط عند ضبط متغيرَي البيئة:
    MEGA_EMAIL     بريد حساب ميجا
    MEGA_PASSWORD  كلمة المرور
    MEGA_FOLDER    (اختياري) اسم المجلد داخل ميجا — الافتراضي "Deep Up"

كل الرفع تشفير من طرف المكتبة (mega.py) قبل الخروج من السيرفر،
ولا تُكتب كلمة المرور في أي سجل أو رد API.
"""
from __future__ import annotations

import base64
import os
import sys
import threading
import time
from typing import Any, Callable

EMAIL = (os.environ.get("MEGA_EMAIL") or "").strip()
PASSWORD = os.environ.get("MEGA_PASSWORD") or ""
FOLDER = (os.environ.get("MEGA_FOLDER") or "Deep Up").strip() or "Deep Up"

_LOCK = threading.Lock()
_CLIENT: Any = None
_CLIENT_AT = 0.0
SESSION_TTL = 25 * 60          # جلسة ميجا ~25 دقيقة ثم نعيد الدخول




# ─────────────────── تحميل مكتبة ميجا عند الطلب ───────────────────
# لا نضع mega.py في requirements.txt إطلاقًا: لو فشل تثبيتها لا يجب أن يفشل
# النشر كله. نُثبّتها وقت أول استخدام داخل مجلد قابل للكتابة، وإن فشلت
# تبقى بقية خدمات السيرفر (تنزيل + تحويل + ترتيب + ZIP) تعمل طبيعيًا.

_LIB_DIR = os.environ.get("MEGA_LIB_DIR") or os.path.join(
    os.environ.get("WORK_DIR", "/tmp/nlm"), "megalib")
_lib_state = {"tried": False, "ok": False, "error": ""}


def _shims() -> None:
    """mega.py 1.0.8 كُتب لبايثون ≤3.9: يحتاج ترميمًا بسيطًا على 3.10+."""
    import asyncio
    import collections
    import collections.abc
    import types
    for name in ("Sequence", "Mapping", "MutableMapping", "Iterable", "Callable",
                 "Set", "MutableSet", "Hashable", "KeysView", "ItemsView", "ValuesView"):
        if not hasattr(collections, name):
            setattr(collections, name, getattr(collections.abc, name))
    if not hasattr(asyncio, "coroutine"):
        asyncio.coroutine = types.coroutine        # tenacity 5.x يستخدمها


def _import_mega() -> Any:
    _shims()
    # mega.py تجرّ نسخة باك بورت قديمة من pathlib (pathlib-1.0.1) تظلّل مكتبة
    # بايثون القياسية بعد إدراج _LIB_DIR في sys.path — فنفشل shutil.move بها.
    # الحل: تثبيت pathlib القياسي في sys.modules قبل أي استيراد من المجلد.
    import pathlib as _stdlib_pathlib
    sys.modules.setdefault("pathlib", _stdlib_pathlib)
    try:
        from mega import Mega          # noqa: F401
        _lib_state.update(tried=True, ok=True, error="")
        return Mega
    except Exception:
        pass
    if not _lib_state["tried"]:
        _lib_state["tried"] = True
        try:
            import subprocess
            os.makedirs(_LIB_DIR, exist_ok=True)
            proc = subprocess.run(
                [sys.executable, "-m", "pip", "install", "--no-input", "--quiet",
                 "--target", _LIB_DIR, "mega.py==1.0.8"],
                capture_output=True, text=True, timeout=300)
            if proc.returncode != 0:
                _lib_state["error"] = (proc.stderr or proc.stdout or "")[-300:].strip()
        except Exception as e:               # noqa: BLE001
            _lib_state["error"] = f"{type(e).__name__}: {e}"[:300]
    if _LIB_DIR not in sys.path:
        sys.path.insert(0, _LIB_DIR)
    try:
        from mega import Mega
        _lib_state.update(ok=True, error="")
        return Mega
    except Exception as e:                   # noqa: BLE001
        _lib_state["ok"] = False
        if not _lib_state["error"]:
            _lib_state["error"] = f"{type(e).__name__}: {e}"[:300]
        raise RuntimeError("مكتبة ميجا غير متاحة: " + _lib_state["error"])


def lib_info() -> dict:
    return {"dir": _LIB_DIR, "ok": _lib_state["ok"],
            "tried": _lib_state["tried"], "error": _lib_state["error"]}


def enabled() -> bool:
    return bool(EMAIL and PASSWORD)


def info() -> dict:
    return {
        "enabled": enabled(),
        "rsa": rsa_available(),
        "sessions": live_sessions(),
        "email_set": bool(EMAIL),
        "folder": FOLDER,
        "library": _library_ok(),
        "lib": lib_info(),
    }


def _library_ok() -> bool:
    if _lib_state["ok"]:
        return True
    try:
        _import_mega()
        return True
    except Exception:
        return False


def _client() -> Any:
    global _CLIENT, _CLIENT_AT
    with _LOCK:
        if _CLIENT is None or (time.time() - _CLIENT_AT) > SESSION_TTL:
            from mega import Mega
            _CLIENT = Mega().login(EMAIL, PASSWORD)
            _CLIENT_AT = time.time()
        return _CLIENT


def _reset() -> None:
    global _CLIENT, _CLIENT_AT
    with _LOCK:
        _CLIENT = None
        _CLIENT_AT = 0.0


def _folder(m: Any) -> Any:
    """معرّف مجلد MEGA (str) أو None للرفع إلى الجذر.

    mega.py: find(name) يعيد (handle, node) — وليس handle مباشرة،
    و create_folder(name) يعيد {name: handle}. الخطأ القديم كان يمرّر أول حرف
    من الـhandle ⇒ MEGA ترد -11 (ENOENT).
    """
    try:
        hit = m.find(FOLDER)
        handle = None
        if isinstance(hit, (tuple, list)) and hit:
            node = hit[1] if len(hit) > 1 else None
            if isinstance(node, dict) and node.get("t") not in (None, 1):
                handle = None                     # ملف وليس مجلدًا
            else:
                handle = hit[0]
        elif isinstance(hit, dict):
            handle = list(hit.keys())[0] if hit else None
        elif isinstance(hit, str):
            handle = hit
        if isinstance(handle, str) and handle:
            return handle
    except Exception:
        pass
    try:
        made = m.create_folder(FOLDER)
        if isinstance(made, dict):
            for _k, v in made.items():
                if isinstance(v, str) and v:
                    return v
    except Exception:
        pass
    return None


def upload(path: str, remote_name: str,
           on_state: Callable[..., None],
           client: Any = None) -> threading.Thread:
    """يرفع الملف في Thread منفصل. on_state(state=..., percent=..., link=..., error=...)."""

    def worker() -> None:
        t0 = time.time()
        try:
            on_state(state="uploading", percent=0)
            m = client if client is not None else _client()
            dest = _folder(m)
            try:
                handle = m.upload(path, dest, dest_filename=remote_name)
            except Exception as first:               # noqa: BLE001
                low = str(first).lower()
                if dest and ("enoent" in low or "-11" in low or "not found" in low):
                    on_state(state="uploading", percent=0,
                             error="مجلد MEGA غير متاح — نرفع إلى الجذر")
                    dest = None
                    handle = m.upload(path, None, dest_filename=remote_name)
                else:
                    raise
            link = ""
            try:
                link = m.get_upload_link(handle) or ""
            except Exception:
                try:
                    link = m.get_link(handle) or ""
                except Exception:
                    link = ""
            if not link:
                raise RuntimeError("تم الرفع لكن تعذّر إنشاء رابط عام")
            on_state(state="link", percent=100, link=link,
                     seconds=round(time.time() - t0, 1))
            # مجلد مشترك: نحاول إنشاء رابط للمجلد لعرض كل الملفات
            try:
                fl = m.get_folder_link(dest) if dest else ""
            except Exception:
                fl = ""
            on_state(state="done", percent=100, link=link, folder_link=fl,
                     seconds=round(time.time() - t0, 1))
        except Exception as e:                       # noqa: BLE001
            _reset()
            msg = f"{type(e).__name__}: {e}"
            low = msg.lower()
            if "enoent" in low or "object (typically" in low:
                msg = "تعذّر رفع الملف إلى MEGA: مسار الحفظ غير صالح (ENOENT -11)"
            elif "password" in low or "credentials" in low or "login" in low:
                msg = "بيانات دخول ميجا غير صحيحة (MEGA_EMAIL / MEGA_PASSWORD)"
            on_state(state="error", percent=0, error=msg[:300],
                     seconds=round(time.time() - t0, 1))

    t = threading.Thread(target=worker, name="mega-upload", daemon=True)
    t.start()
    return t


# ─────────────────── قناة مشفّرة لبيانات الحساب ───────────────────
# الهاتف يشفّر (RSA-OAEP) ⇒ السيرفر يفك ⇒ ميجا تعيد التشفير داخليًا عند الدخول
# ⇒ نمسح النص الصريح من الذاكرة. لا يُكتب أي شيء على القرص.

try:
    from Crypto.PublicKey import RSA as _RSA
    from Crypto.Cipher import PKCS1_OAEP as _OAEP
    _KEY = _RSA.generate(2048)
    _RSA_OK = True
except Exception:                      # noqa: BLE001
    _KEY = None
    _RSA_OK = False

SESSIONS: dict[str, dict] = {}
SESS_TTL = int(os.environ.get("MEGA_SESS_TTL_MIN", "720")) * 60   # 12 ساعة افتراضيًا


def rsa_available() -> bool:
    return _RSA_OK


def public_pem() -> str:
    if not _RSA_OK:
        raise RuntimeError("وحدة التشفير غير متاحة على السيرفر (pycryptodome)")
    return _KEY.publickey().export_key().decode("ascii")


def open_sealed(sealed_b64: str) -> tuple[str, str]:
    """يفك ظرف الهاتف ويعيد (email, password). يمسح المخزن المؤقت بعد الاستخدام."""
    if not _RSA_OK:
        raise RuntimeError("وحدة التشفير غير متاحة على السيرفر")
    raw = base64.b64decode(sealed_b64 or "")
    buf = bytearray(_OAEP.new(_KEY).decrypt(raw))
    try:
        text = buf.decode("utf-8")
        email, _, password = text.partition("\n")
        return email.strip(), password
    finally:
        for i in range(len(buf)):      # تصفير الذاكرة
            buf[i] = 0
        del raw


def mask(email: str) -> str:
    if not email:
        return "•"
    name, _, dom = email.partition("@")
    keep = name[:2] if len(name) > 2 else name[:1]
    return keep + "•" * max(2, len(name) - len(keep)) + ("@" + dom if dom else "")


def create_session(email: str, password: str) -> str:
    if FAKE:
        sid = base64.urlsafe_b64encode(os.urandom(18)).decode().rstrip("=")
        SESSIONS[sid] = _fake_session()
        return sid
    Mega = _import_mega()
    m = Mega().login(email, password)          # ميجا تُشفّر بيانات الدخول داخليًا
    sid = base64.urlsafe_b64encode(os.urandom(18)).decode().rstrip("=")
    SESSIONS[sid] = {"client": m, "at": time.time(),
                     "email": mask(email), "uploads": 0}
    return sid


def session(sid: str) -> dict | None:
    s0 = SESSIONS.get(sid)
    if not s0:
        return None
    if time.time() - s0["at"] > SESS_TTL:
        drop_session(sid)
        return None
    return s0


def drop_session(sid: str) -> bool:
    s0 = SESSIONS.pop(sid, None)
    if not s0:
        return False
    try:
        s0["client"] = None        # إسقاط مرجع عميل ميجا
    except Exception:
        pass
    return True


def live_sessions() -> int:
    for k in [k for k, v in SESSIONS.items() if time.time() - v["at"] > SESS_TTL]:
        drop_session(k)
    return len(SESSIONS)


# ─────────────── Deep Up: رفع ملف واحد إلى مجلد باسم محدّد ───────────────
_LAST_LINK: dict[str, str] = {"link": ""}


def last_link() -> str:
    return _LAST_LINK.get("link", "")


def ensure_named_folder(m: Any, name: str) -> str | None:
    """يجد/يُنشئ مجلدًا باسم `name` في جذر الحساب ويعيد الـhandle."""
    name = (name or FOLDER).strip() or FOLDER
    try:
        hit = m.find(name)
        handle = None
        if isinstance(hit, (tuple, list)) and hit:
            node = hit[1] if len(hit) > 1 else None
            if isinstance(node, dict) and node.get("t") not in (None, 1):
                handle = None
            else:
                handle = hit[0]
        elif isinstance(hit, dict):
            handle = list(hit.values())[0] if hit else None
        elif isinstance(hit, str):
            handle = hit
        if isinstance(handle, str) and handle:
            return handle
    except Exception:
        pass
    try:
        made = m.create_folder(name)
        if isinstance(made, dict):
            for _k, v in made.items():
                if isinstance(v, str) and v:
                    return v
    except Exception:
        pass
    return None


def upload_file(client: Any, path: str, remote_name: str, folder_name: str,
                on_state: Callable[..., None]) -> str:
    """يرفع ملفًا واحدًا (تنفيذ متزامن) إلى مجلد الحساب ويعيد الرابط العام."""
    m = client if client is not None else _client()
    dest = ensure_named_folder(m, folder_name)
    on_state(state="uploading", folder=folder_name or FOLDER,
             dest=bool(dest), size=os.path.getsize(path) if os.path.exists(path) else 0)
    try:
        handle = m.upload(path, dest, dest_filename=remote_name)
    except Exception:
        if dest:                                    # خطة بديلة: جذر الحساب
            on_state(state="uploading", folder="(الجذر)")
            handle = m.upload(path, None, dest_filename=remote_name)
        else:
            raise
    link = ""
    for getter in ("get_upload_link", "get_link"):
        try:
            link = getattr(m, getter)(handle) or ""
        except Exception:
            link = ""
        if link:
            break
    if not link:
        raise RuntimeError("تم الرفع لكن تعذّر إنشاء رابط عام للملف")
    _LAST_LINK["link"] = link
    return link


def upload_with(client: Any, path: str, remote_name: str, folder_name: str,
                on_state: Callable[..., None]) -> str:
    return upload_file(client, path, remote_name, folder_name, on_state)


# ─────────────── وضع محاكاة ميجا للاختبار المحلي (DEEPUP_FAKE_MEGA=1) ───────────────
FAKE = os.environ.get("DEEPUP_FAKE_MEGA", "") == "1"
FAKE_STORE: dict[str, dict] = {}


if FAKE:
    class _FakeMega:                                # noqa: D401
        def find(self, name):
            for h, n in FAKE_STORE.items():
                if n["type"] == "dir" and n["name"] == name and n["parent"] is None:
                    return (h, {"t": 1, "a": {"n": name}})
            return None

        def create_folder(self, name, dest=None):
            h = "d%d" % (len(FAKE_STORE) + 1)
            FAKE_STORE[h] = {"type": "dir", "name": name, "parent": dest}
            return {name: h}

        def upload(self, path, dest, dest_filename=None):
            h = "f%d" % (len(FAKE_STORE) + 1)
            FAKE_STORE[h] = {"type": "file", "name": dest_filename,
                             "parent": dest, "size": os.path.getsize(path)}
            return h

        def get_upload_link(self, handle):
            return "https://mega.nz/file/" + handle + "FAKEKEY"

        def get_folder_link(self, handle):
            return "https://mega.nz/folder/" + str(handle)

        def get_public_url_info(self, url):
            return {"size": 262144, "name": "ملف-اختبار-ميجا.zip"}

        def download_url(self, url, dest_path=None, dest_filename=None):
            p = os.path.join(dest_path or ".", dest_filename or "mega.bin")
            with open(p, "wb") as fh:
                fh.write(b"MEGAFAKE" * 32768)   # ‏262,144 بايت بالضبط
            return p

    def _fake_session() -> dict:
        return {"client": _FakeMega(), "at": time.time(), "email": "fa***@test", "uploads": 0}


def seal(email: str, password: str) -> str:
    """تشفير للاختبار المحلي فقط (التطبيق يشفّر على الهاتف بمفتاح السيرفر العام)."""
    if not _RSA_OK:
        raise RuntimeError("pycryptodome غير مثبت")
    return base64.b64encode(_OAEP.new(_KEY.publickey()).encrypt(
        (email + "\n" + password).encode("utf-8"))).decode()
