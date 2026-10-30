import os
import time
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
TABLE = "jobs"

# คอลัมน์ในตาราง jobs (ดู supabase_schema.sql)
FIELDS = [
    "job_code", "date", "time", "raw_text", "pickup", "dropoff", "flight",
    "order_no", "car_code", "price", "formatted_summary", "sender_id", "status",
]


def enabled():
    return bool(SUPABASE_URL and SUPABASE_KEY)


def _headers(extra=None):
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if extra:
        h.update(extra)
    return h


def _url():
    return f"{SUPABASE_URL}/rest/v1/{TABLE}"


def _to_row(item, sender_id=None):
    return {
        "job_code": item.get("id"),
        "date": item.get("date"),
        "time": item.get("time"),
        "raw_text": item.get("raw_text"),
        "pickup": item.get("pickup"),
        "dropoff": item.get("dropoff"),
        "flight": item.get("flight"),
        "order_no": item.get("order"),
        "car_code": item.get("car_code"),
        "price": item.get("price"),
        "formatted_summary": item.get("formatted_summary"),
        "sender_id": sender_id,
        "status": item.get("status", "รอส่ง"),
    }


def _to_item(row):
    return {
        "db_id": row["id"],
        "id": row.get("job_code"),
        "date": row.get("date"),
        "time": row.get("time"),
        "text": row.get("raw_text"),
        "raw_text": row.get("raw_text"),
        "pickup": row.get("pickup"),
        "pickup_display": row.get("pickup"),
        "dropoff": row.get("dropoff"),
        "dropoff_display": row.get("dropoff"),
        "flight": row.get("flight"),
        "order": row.get("order_no"),
        "car_code": row.get("car_code"),
        "price": row.get("price"),
        "formatted_summary": row.get("formatted_summary"),
        "sender_id": row.get("sender_id"),
        "status": row.get("status"),
    }


def insert_job(item, sender_id=None, retries=3):
    """บันทึกใบงานใหม่ คืน db_id (ถ้าล้มเหลวทุกครั้งคืน None) ลองซ้ำเมื่อเน็ตสะดุด"""
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(
                _url(),
                headers=_headers({"Prefer": "return=representation"}),
                json=_to_row(item, sender_id),
                timeout=10,
            )
            r.raise_for_status()
            return r.json()[0]["id"]
        except Exception as e:
            print(f"❌ Supabase insert ล้มเหลว (ครั้งที่ {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(1)
    return None


def load_pending():
    """ใบงานที่ยังไม่ส่ง เรียงตามเวลาที่เข้า"""
    return _select({"status": "eq.รอส่ง", "order": "id.asc"})


def load_latest(limit=50):
    """ใบงานล่าสุดสำหรับหน้าเว็บ (ใหม่สุดก่อน)"""
    return _select({"order": "id.desc", "limit": str(limit)})


def _select(params):
    try:
        r = requests.get(_url(), headers=_headers(), params=params, timeout=10)
        r.raise_for_status()
        return [_to_item(row) for row in r.json()]
    except Exception as e:
        print(f"❌ Supabase select ล้มเหลว: {e}")
        return []


def mark_sent(db_ids):
    """เปลี่ยนสถานะเป็น 'ส่งแล้ว' คืน True ถ้าสำเร็จ"""
    ids = [i for i in db_ids if i]
    if not ids:
        return True
    try:
        r = requests.patch(
            _url(),
            headers=_headers(),
            params={"id": f"in.({','.join(str(i) for i in ids)})"},
            json={"status": "ส่งแล้ว", "sent_at": "now()"},
            timeout=10,
        )
        r.raise_for_status()
        return True
    except Exception as e:
        print(f"❌ Supabase mark_sent ล้มเหลว: {e}")
        return False


def delete_jobs(target):
    """ลบใบงานที่ job_code หรือ order_no ตรงกันทั้งตัว (ไม่ใช่บางส่วน)"""
    try:
        r = requests.delete(
            _url(),
            headers=_headers({"Prefer": "return=representation"}),
            params={"or": f"(job_code.eq.{target},order_no.eq.{target})"},
            timeout=10,
        )
        r.raise_for_status()
        return len(r.json())
    except Exception as e:
        print(f"❌ Supabase delete ล้มเหลว: {e}")
        return 0


def clear_pending():
    try:
        r = requests.delete(
            _url(),
            headers=_headers({"Prefer": "return=representation"}),
            params={"status": "eq.รอส่ง"},
            timeout=10,
        )
        r.raise_for_status()
        return len(r.json())
    except Exception as e:
        print(f"❌ Supabase clear ล้มเหลว: {e}")
        return 0
