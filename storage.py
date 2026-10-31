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
import shutil
import threading
import time

import requests

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = (
    os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    or os.getenv("SUPABASE_KEY", "").strip()
)
TABLE = os.getenv("SUPABASE_TABLE", "bot_store").strip() or "bot_store"
# อิงโฟลเดอร์ของโปรแกรม (เปิดบอทจากโฟลเดอร์อื่นก็ยังอ่านไฟล์เดิม ไม่ใช่ไฟล์ว่างไฟล์ใหม่)
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_FILE = os.getenv("LOCAL_STORE_FILE", "").strip() or os.path.join(_BASE_DIR, "local_store.json")
LEGACY_SETTINGS_FILE = os.path.join(_BASE_DIR, "settings.json")
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
    except Exception as e:
        # ไฟล์เสีย: ห้ามคืนค่าว่าง (จะถูกบันทึกทับจนข้อมูลหาย) เก็บไฟล์เสียไว้กู้คืน แล้วแจ้งข้อผิดพลาด
        bad = f"{LOCAL_FILE}.bad-{time.strftime('%Y%m%d-%H%M%S')}"
        try:
            os.replace(LOCAL_FILE, bad)
        except OSError:
            bad = LOCAL_FILE
        raise StorageError(f"อ่าน {os.path.basename(LOCAL_FILE)} ไม่ได้ ({e}) เก็บไฟล์เดิมไว้ที่ {bad} "
                           f"กู้คืนได้จาก {os.path.basename(LOCAL_FILE)}.bak")


def _write_local(data):
    tmp = LOCAL_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    if os.path.exists(LOCAL_FILE):  # สำรองฉบับก่อนหน้าไว้ 1 ชุด
        shutil.copy2(LOCAL_FILE, LOCAL_FILE + ".bak")
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



# ---------------- ตารางอื่นๆ (เช่น jobs) ----------------
def _table_endpoint(table):
    return f"{SUPABASE_URL}/rest/v1/{table}"


def upsert_rows(table, rows, on_conflict):
    """เพิ่ม/อัปเดตหลายแถวในตาราง (ใช้เฉพาะ Supabase) ถ้าไม่ได้ใช้ Supabase จะไม่ทำอะไร"""
    if not rows or not using_supabase():
        return False
    if config_problem():
        raise StorageError(config_problem())
    try:
        r = requests.post(
            _table_endpoint(table),
            headers=_headers({"Prefer": "resolution=merge-duplicates,return=minimal"}),
            params={"on_conflict": on_conflict},
            data=json.dumps(rows, ensure_ascii=False).encode("utf-8"),
            timeout=TIMEOUT,
        )
    except requests.RequestException as e:
        raise StorageError(f"เชื่อมต่อ Supabase ไม่ได้: {e}")
    if r.status_code not in (200, 201, 204):
        raise StorageError(f"Supabase ({table}) ตอบกลับ {r.status_code}: {r.text[:200]}")
    return True


def delete_rows(table, column, value):
    """ลบแถวที่ column = value ในตาราง (ใช้เฉพาะ Supabase) คืน True ถ้าลบแล้ว"""
    if not using_supabase():
        return False
    if config_problem():
        raise StorageError(config_problem())
    try:
        r = requests.delete(
            _table_endpoint(table),
            headers=_headers({"Prefer": "return=representation"}),
            params={column: f"eq.{value}"},
            timeout=TIMEOUT,
        )
    except requests.RequestException as e:
        raise StorageError(f"เชื่อมต่อ Supabase ไม่ได้: {e}")
    if r.status_code not in (200, 204):
        raise StorageError(f"Supabase ({table}) ตอบกลับ {r.status_code}: {r.text[:200]}")
    try:
        return bool(r.json())
    except ValueError:
        return True


def select_rows(table, params):
    """อ่านแถวจากตาราง คืน list (ว่างถ้าไม่ได้ใช้ Supabase)"""
    if not using_supabase():
        return []
    if config_problem():
        raise StorageError(config_problem())
    try:
        r = requests.get(_table_endpoint(table), headers=_headers(), params=params, timeout=TIMEOUT)
    except requests.RequestException as e:
        raise StorageError(f"เชื่อมต่อ Supabase ไม่ได้: {e}")
    if r.status_code != 200:
        raise StorageError(f"Supabase ({table}) ตอบกลับ {r.status_code}: {r.text[:200]}")
    return r.json()


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
