"""
ที่เก็บข้อมูลถาวรของระบบ (Supabase)

- ถ้าตั้งค่า SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY ไว้ -> เก็บทุกอย่างลง Supabase
  (ตั้งค่า, กลุ่ม LINE, ถนนหลัก, ย่านสำคัญ, คิวใบงาน, ประวัติใบงาน)
  เซิร์ฟเวอร์หลับ/รีสตาร์ท/Deploy ใหม่ ข้อมูลก็ไม่หาย
- ถ้าไม่ได้ตั้งค่า -> เก็บลงไฟล์ในเครื่อง (local_store.json) เหมือนเดิม ใช้ตอนทดสอบในเครื่อง

ตารางที่ใช้ (สร้างด้วยไฟล์ supabase_setup.sql):
    bot_store(key text primary key, value jsonb, updated_at timestamptz)
"""
import os
import json
import threading

import requests

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    or os.getenv("SUPABASE_KEY", "").strip()
)
TABLE = os.getenv("SUPABASE_TABLE", "bot_store").strip() or "bot_store"
LOCAL_FILE = "local_store.json"
LEGACY_SETTINGS_FILE = "settings.json"
TIMEOUT = 10

_local_lock = threading.Lock()


class StorageError(Exception):
    pass


def config_problem():
    """เช็คว่าใส่ค่าใน Render ถูกช่องไหม คืนข้อความปัญหา หรือ "" ถ้าโอเค"""
    if not SUPABASE_URL and not SUPABASE_KEY:
        return ""
    if not SUPABASE_URL.startswith("https://"):
        return ("SUPABASE_URL ต้องเป็นลิงก์แบบ https://xxxx.supabase.co "
                "(ตอนนี้ใส่ค่าอื่นไว้ เช่น คีย์)")
    if SUPABASE_KEY.startswith("sb_publishable_"):
        return ("SUPABASE_SERVICE_ROLE_KEY ใส่ publishable key ไว้ ต้องใช้ secret key "
                "(ขึ้นต้นด้วย sb_secret_) หรือ service_role key")
    if not SUPABASE_KEY:
        return "ยังไม่ได้ใส่ SUPABASE_SERVICE_ROLE_KEY"
    return ""


def using_supabase():
    return bool(SUPABASE_URL and SUPABASE_KEY)


def _headers(extra=None):
    h = {
        "apikey": SUPABASE_KEY,
        "Content-Type": "application/json",
    }
    # คีย์แบบเก่า (service_role) เป็น JWT ขึ้นต้นด้วย eyJ ต้องส่ง Authorization ด้วย
    # คีย์แบบใหม่ (sb_secret_...) ส่งแค่ apikey ก็พอ
    if SUPABASE_KEY.startswith("eyJ"):
        h["Authorization"] = f"Bearer {SUPABASE_KEY}"
    if extra:
        h.update(extra)
    return h


def _endpoint():
    return f"{SUPABASE_URL}/rest/v1/{TABLE}"


# ---------------- Local file (ใช้เมื่อไม่มี Supabase) ----------------
def _read_local():
    if not os.path.exists(LOCAL_FILE):
        return {}
    try:
        with open(LOCAL_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_local(data):
    tmp = LOCAL_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, LOCAL_FILE)


def _read_legacy_settings():
    if os.path.exists(LEGACY_SETTINGS_FILE):
        try:
            with open(LEGACY_SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None


# ---------------- Public API ----------------
def get(key, default=None):
    """อ่านค่า ถ้าไม่มีข้อมูลคืน default / ถ้าเชื่อมต่อไม่ได้จะ raise StorageError"""
    if using_supabase():
        if config_problem():
            raise StorageError(config_problem())
        try:
            r = requests.get(
                _endpoint(),
                headers=_headers(),
                params={"key": f"eq.{key}", "select": "value"},
                timeout=TIMEOUT,
            )
        except requests.RequestException as e:
            raise StorageError(f"เชื่อมต่อ Supabase ไม่ได้: {e}")
        if r.status_code != 200:
            raise StorageError(f"Supabase ตอบกลับ {r.status_code}: {r.text[:200]}")
        rows = r.json()
        if not rows:
            return default
        return rows[0].get("value", default)

    with _local_lock:
        data = _read_local()
    if key in data:
        return data[key]
    if key == "settings":
        legacy = _read_legacy_settings()
        if legacy is not None:
            return legacy
    return default


def set(key, value):
    """บันทึกค่า (ถ้ามีอยู่แล้วจะเขียนทับ) / ถ้าบันทึกไม่ได้จะ raise StorageError"""
    if using_supabase():
        if config_problem():
            raise StorageError(config_problem())
        try:
            r = requests.post(
                _endpoint(),
                headers=_headers({"Prefer": "resolution=merge-duplicates,return=minimal"}),
                params={"on_conflict": "key"},
                data=json.dumps([{"key": key, "value": value}], ensure_ascii=False).encode("utf-8"),
                timeout=TIMEOUT,
            )
        except requests.RequestException as e:
            raise StorageError(f"เชื่อมต่อ Supabase ไม่ได้: {e}")
        if r.status_code not in (200, 201, 204):
            raise StorageError(f"Supabase ตอบกลับ {r.status_code}: {r.text[:200]}")
        return True

    with _local_lock:
        data = _read_local()
        data[key] = value
        _write_local(data)
    return True


def health():
    """เช็คว่าที่เก็บข้อมูลใช้งานได้ไหม คืน (ok, ข้อความ)"""
    problem = config_problem()
    if problem:
        return False, problem
    if not using_supabase():
        return False, "ยังไม่ได้ตั้งค่า Supabase (ใช้ไฟล์ในเครื่อง ข้อมูลจะหายเมื่อเซิร์ฟเวอร์รีสตาร์ท)"
    try:
        get("__ping__")
        return True, "เชื่อมต่อ Supabase ได้"
    except StorageError as e:
        return False, str(e)
