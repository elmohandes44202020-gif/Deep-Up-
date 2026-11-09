"""Deep Up — اختبار شامل: رابط ⇒ تنزيل على السيرفر ⇒ رفع إلى ميجا (محاكاة)."""
import functools, http.server, json, os, socketserver, subprocess, sys, threading, time
import httpx
import main as server_mod   # لاختبار _resolve_url مباشرةً

HERE = os.path.dirname(os.path.abspath(__file__))
FIX = "/tmp/deepup-fix"; os.makedirs(FIX, exist_ok=True)
open(os.path.join(FIX, "محاضرة-الفيزياء.mp3"), "wb").write(b"A" * 300_000)
open(os.path.join(FIX, "report.pdf"), "wb").write(b"%PDF-1.4\n" + b"B" * 120_000)
# صفحة HTML تحاكي رابطًا يُرجع «عرض ويب» بدل الملف — يجب أن تُرفض بوضوح
open(os.path.join(FIX, "page.html"), "w", encoding="utf-8").write(
    "<!DOCTYPE html><html><head><title>معاينة</title></head><body>هذه صفحة ويب وليست ملفًا</body></html>")
# صفحة «حل ذكي»: رابط تحميل مباشر داخل صفحة HTML
open(os.path.join(FIX, "page-with-dl.html"), "w", encoding="utf-8").write(
    '<!DOCTYPE html><html><body><h1>الملف</h1>'
    '<a href="%D9%85%D8%AD%D8%A7%D8%B6%D8%B1%D8%A9-%D8%A7%D9%84%D9%81%D9%8A%D8%B2%D9%8A%D8%A7%D8%A1.mp3">⬇ تحميل مباشر</a>'
    "</body></html>")
# صفحة بلا أي رابط تحميل — يجب أن يفشل الحل الذكي برسالة واضحة
open(os.path.join(FIX, "page-no-dl.html"), "w", encoding="utf-8").write(
    "<!DOCTYPE html><html><body><p>لا يوجد هنا أي رابط ملف</p></body></html>")
PS, PA, KEY = 9111, 9112, "deep-key"
PF = 9113   # خادم "متقطّع" يفشل مرتين ثم يستجيب — لاختبار إعادة المحاولة

def serve():
    h = functools.partial(http.server.SimpleHTTPRequestHandler, directory=FIX)
    socketserver.TCPServer.allow_reuse_address = True
    s = socketserver.TCPServer(("127.0.0.1", PS), h)
    threading.Thread(target=s.serve_forever, daemon=True).start(); return s

def serve_flaky():
    hits = {"n": 0}
    class Flaky(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            hits["n"] += 1
            if hits["n"] <= 2:
                self.send_error(500, "temporary failure")
                return
            body = b"FLAKY-" + b"C" * 150_000
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *a):   # صامت
            pass
    socketserver.TCPServer.allow_reuse_address = True
    s = socketserver.TCPServer(("127.0.0.1", PF), Flaky)
    threading.Thread(target=s.serve_forever, daemon=True).start(); return s

def main():
    srv = serve()
    flaky = serve_flaky()
    env = {**os.environ, "API_KEYS": f"me:{KEY}", "WORK_DIR": "/tmp/deepup-work",
           "ALLOW_PRIVATE_HOSTS": "1", "DEEPUP_FAKE_MEGA": "1", "MAX_FILE_MB": "50"}
    api = subprocess.Popen([sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
                            "--port", str(PA), "--log-level", "warning"], cwd=HERE, env=env)
    base = f"http://127.0.0.1:{PA}"; H = {"X-Api-Key": KEY}

    def wait_done(ids):
        d = {}
        for _ in range(300):
            d = {j["id"]: j for j in httpx.get(base + "/jobs", headers=H).json()["jobs"]}
            if all(i in d and d[i]["state"] in ("done", "error") for i in ids):
                return d
            time.sleep(0.2)
        return d

    try:
        for _ in range(100):
            try:
                if httpx.get(base + "/healthz", timeout=1).status_code == 200: break
            except Exception: time.sleep(0.2)
        print("· /healthz:", httpx.get(base + "/healthz").json())
        assert httpx.get(base + "/healthz").json()["version"] == "1.2.0"
        print("· /diag:", {k: v for k, v in httpx.get(base + "/diag", headers=H).json().items()
                           if k in ("version", "rsa", "sessions", "keys")})
        assert httpx.post(base + "/jobs", json={"url": "https://x/y"}).status_code == 401
        print("✓ رفض بدون مفتاح (401)")

        # مفتاح RSA + جلسة ميجا مشفّرة
        k = httpx.get(base + "/mega/key", headers=H).json()
        pub = k["pubkey"]
        assert pub.startswith("-----BEGIN PUBLIC KEY-----"), pub[:30]
        import base64
        from Crypto.PublicKey import RSA
        from Crypto.Cipher import PKCS1_OAEP
        pubkey = RSA.import_key(pub)
        sealed = base64.b64encode(PKCS1_OAEP.new(pubkey).encrypt(
            b"user@example.com\nS3cretPass!")).decode()
        r = httpx.post(base + "/mega/session", headers=H, json={"sealed": sealed}, timeout=30)
        print("✓ /mega/session →", r.status_code, {kk: r.json().get(kk) for kk in ("email", "expires_in_sec")})
        assert r.status_code == 200, r.text
        sid = r.json()["session_id"]

        # رابط ملف صوتي
        j1 = httpx.post(base + "/jobs", headers=H, timeout=30,
                        json={"url": f"http://127.0.0.1:{PS}/%D9%85%D8%AD%D8%A7%D8%B6%D8%B1%D8%A9-%D8%A7%D9%84%D9%81%D9%8A%D8%B2%D9%8A%D8%A7%D8%A1.mp3",
                              "session_id": sid}).json()
        # رابط مستند
        j2 = httpx.post(base + "/jobs", headers=H, timeout=30,
                        json={"url": f"http://127.0.0.1:{PS}/report.pdf", "session_id": sid}).json()
        print("✓ أُنشئت مهمتان:", j1["id"], j2["id"])

        stages = {}
        for _ in range(200):
            d = {j["id"]: j for j in httpx.get(base + "/jobs", headers=H).json()["jobs"]}
            for jid, j in d.items():
                stages.setdefault(jid, []).append((j["state"], j["percent"]))
            if all(j["state"] in ("done", "error") for j in d.values()): break
            time.sleep(0.2)
        for jid, j in d.items():
            print(f"  · {j['filename']}: state={j['state']} percent={j['percent']} "
                  f"size={j['total']} link={j['link']} stage={j['stage']}")
            assert j["state"] == "done", j
            assert j["link"].startswith("https://mega.nz/file/"), j["link"]
            assert j["total"] > 0 and j["percent"] == 100
        assert d[j1["id"]]["filename"].startswith("محاضرة-الفيزياء"), d[j1["id"]]["filename"]
        # ── حل الروابط إلى تحميل مباشر (وحدة) ──
        assert "drive.usercontent.google.com" in server_mod._resolve_url("https://drive.google.com/file/d/1AbC_xYz-123/view?usp=sharing")
        assert "id=1AbC_xYz-123" in server_mod._resolve_url("https://drive.google.com/file/d/1AbC_xYz-123/view?usp=sharing")
        assert "id=ABC123" in server_mod._resolve_url("https://drive.google.com/open?id=ABC123")
        assert "dl=1" in server_mod._resolve_url("https://www.dropbox.com/s/abc/file.zip?dl=0")
        assert server_mod._resolve_url("http://127.0.0.1:9111/report.pdf").endswith("/report.pdf")
        print("✓ _resolve_url: Drive → رابط مباشر · Dropbox → dl=1")

        # ── حظر رفع صفحات HTML بدل الملفات (تكاملي) ──
        files_before = len([n for n in httpx.get(base + "/_fake/store", headers=H).json()["nodes"].values()
                            if n["type"] == "file"])
        hj = httpx.post(base + "/jobs", headers=H, timeout=30,
                        json={"url": f"http://127.0.0.1:{PS}/page.html",
                              "session_id": sid, "folder": "Deep Up HTML"}).json()
        d3 = wait_done([hj["id"]])
        assert d3[hj["id"]]["state"] == "error", d3[hj["id"]]
        assert "HTML" in d3[hj["id"]]["error"], d3[hj["id"]]["error"]
        files_after = len([n for n in httpx.get(base + "/_fake/store", headers=H).json()["nodes"].values()
                           if n["type"] == "file"])
        assert files_after == files_before, "صفحة HTML حُمّلت ورُفعت كملف!"
        print("✓ صفحات HTML تُرفض برسالة واضحة ولا تُرفع إلى ميجا إطلاقًا")

        # ── رابط mega.nz عام (محاكاة) — يعمل بلا أي تغيير في التطبيق ──
        mj = httpx.post(base + "/jobs", headers=H, timeout=30,
                        json={"url": "https://mega.nz/file/abc123handle#AAAAAAAAAAAAAAAAAAAAAA",
                              "session_id": sid, "folder": "Deep Up Mega"}).json()
        d4 = wait_done([mj["id"]])
        assert d4[mj["id"]]["state"] == "done", d4[mj["id"]]
        assert d4[mj["id"]]["total"] == 262144, d4[mj["id"]]          # حجم الملف المحاكى
        assert d4[mj["id"]]["filename"] == "ملف-اختبار-ميجا.zip", d4[mj["id"]]["filename"]
        assert d4[mj["id"]]["link"].startswith("https://mega.nz/file/")
        print("✓ رابط mega.nz العام: نُزّل بلا حساب ورُفع — الاسم:", d4[mj["id"]]["filename"])

        # ── «حل ذكي ✨» اختياري: صفحة بها رابط تحميل ──
        sj = httpx.post(base + "/jobs", headers=H, timeout=30,
                        json={"url": f"http://127.0.0.1:{PS}/page-with-dl.html",
                              "session_id": sid, "smart": True, "folder": "Deep Up Smart"}).json()
        d5 = wait_done([sj["id"]])
        assert d5[sj["id"]]["state"] == "done", d5[sj["id"]]
        assert d5[sj["id"]]["filename"].startswith("محاضرة-الفيزياء"), d5[sj["id"]]["filename"]
        assert d5[sj["id"]]["resolved_from"].endswith("page-with-dl.html")
        print("✓ الحل الذكي: وُجد رابط التحميل داخل الصفحة ورُفع:", d5[sj["id"]]["filename"])

        # صفحة بلا روابط + حل ذكي ⇒ خطأ واضح
        nj = httpx.post(base + "/jobs", headers=H, timeout=30,
                        json={"url": f"http://127.0.0.1:{PS}/page-no-dl.html",
                              "session_id": sid, "smart": True}).json()
        d6 = wait_done([nj["id"]])
        assert d6[nj["id"]]["state"] == "error" and "رابط تحميل" in d6[nj["id"]]["error"], d6[nj["id"]]
        print("✓ الحل الذكي: صفحة بلا روابط تحميل ⇒ خطأ واضح دون رفع أي شيء")

        # نفس الصفحة بدون تفعيل الحل الذكي ⇒ رسالة HTML المعتادة
        bj = httpx.post(base + "/jobs", headers=H, timeout=30,
                        json={"url": f"http://127.0.0.1:{PS}/page-with-dl.html", "session_id": sid}).json()
        d7 = wait_done([bj["id"]])
        assert d7[bj["id"]]["state"] == "error" and "HTML" in d7[bj["id"]]["error"], d7[bj["id"]]
        print("✓ بدون «حل ذكي»: السلوك القديم كما هو (خطأ HTML واضح)")

        print("✓ كل مهمة: نُزّلت على السيرفر ثم رُفعت إلى مجلد Deep Up على ميجا")

        # ── /jobs/batch: دفعة روابط (صالح + مكرر + ftp مرفوض) ──
        fb = httpx.post(base + "/jobs/batch", headers=H, timeout=30, json={
            "urls": [f"http://127.0.0.1:{PS}/report.pdf",
                     f"ftp://example.com/x.bin",
                     f"http://127.0.0.1:{PS}/report.pdf",
                     f"http://127.0.0.1:{PS}/محاضرة-الفيزياء.mp3"],
            "session_id": sid, "folder": "Deep Up Batch"}).json()
        print("✓ /jobs/batch → أُنشئ", fb["count"], "ورُفض", [r["url"] for r in fb["rejected"]])
        assert fb["count"] == 2, fb
        assert len(fb["rejected"]) == 1 and "http" in fb["rejected"][0]["reason"].lower()
        assert httpx.post(base + "/jobs/batch", headers=H, json={"urls": ["", "  "]}).status_code == 400
        print("✓ /jobs/batch: رفض دفعة فارغة وأزال التكرار ورفض ftp")

        # ── إعادة المحاولة التلقائية: خادم يفشل مرتين ثم يستجيب ──
        rj = httpx.post(base + "/jobs", headers=H, timeout=30,
                        json={"url": f"http://127.0.0.1:{PF}/flaky.bin",
                              "session_id": sid, "folder": "Deep Up Retry"}).json()
        print("✓ أُنشئت مهمة إعادة المحاولة:", rj["id"])

        batch_ids = [j["id"] for j in fb["created"]]
        d2 = wait_done(batch_ids + [rj["id"]])
        for jid in batch_ids:
            print(f"  · دفعة {jid}: {d2[jid]['state']} → {d2[jid]['filename']}")
            assert d2[jid]["state"] == "done", d2[jid]
        assert d2[rj["id"]]["state"] == "done", d2[rj["id"]]
        print(f"✓ إعادة المحاولة التلقائية نجحت: state={d2[rj['id']]['state']} "
              f"attempts={d2[rj['id']]['attempts']} (خادمان يُرجعان 500 مرتين)")
        assert d2[rj["id"]]["attempts"] >= 3, d2[rj["id"]]

        # الملفات وصلت لمجلد واحد باسم Deep Up
        store = httpx.get(base + "/_fake/store", headers=H).json()["nodes"]
        dirs = {h: n for h, n in store.items() if n["type"] == "dir"}
        files = {h: n for h, n in store.items() if n["type"] == "file"}
        root = next((h for h, n in dirs.items() if n["name"] == "Deep Up"), None)
        assert root, "لم يُنشأ مجلد Deep Up"
        inside = [n for n in files.values() if n["parent"] == root]
        print("· شجرة ميجا (محاكاة):  Deep Up/ →", [n["name"] for n in inside])
        assert len(inside) == 2, inside
        assert not [n for n in files.values() if n["parent"] is None], "ملفات في الجذر!"

        # لا نسخة متبقية على القرص
        left = [f for f in os.listdir("/tmp/deepup-work") if f.endswith(".bin")]
        print("· ملفات مؤقتة متبقية على السيرفر:", left)
        assert left == [], left
        print("✓ السيرفر لا يُبقي نسخة من ملفاتك بعد الرفع")

        # حذف مهمة
        print("✓ حذف مهمة:", httpx.delete(f"{base}/jobs/{j1['id']}", headers=H).json())
        assert httpx.get(f"{base}/jobs/{j1['id']}", headers=H).status_code == 404
        print("\nكل اختبارات Deep Up نجحت ✅")
        return 0
    finally:
        api.terminate(); srv.shutdown(); flaky.shutdown()

raise SystemExit(main())
