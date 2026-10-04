import os
import re
import json
import copy
import time
import threading
import datetime

import requests
from flask import Flask, request, abort, render_template, jsonify
from dotenv import load_dotenv

# Import LINE SDK (v3)
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, PushMessageRequest, ReplyMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

load_dotenv()

# โหลด .env ก่อน แล้วค่อย import โมดูลที่อ่านค่า env
import storage
from google_sheet import (
    save_to_google_sheets, get_sheet_client, delete_row_from_google_sheets, has_credentials
)

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "")

handler = WebhookHandler(LINE_CHANNEL_SECRET)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)

# เวลาไทย (เซิร์ฟเวอร์ Render ใช้เวลา UTC)
TH_TZ = datetime.timezone(datetime.timedelta(hours=7))


def now_th():
    return datetime.datetime.now(TH_TZ)


# Global State (โหลดจาก Supabase ตอนเปิดเครื่อง และบันทึกกลับทุกครั้งที่เปลี่ยน)
state_lock = threading.RLock()
job_queue = []
timer_thread = None
timer_start_time = None
latest_jobs = []
last_sender_id = None
MAX_HISTORY = 100

# ค่าเริ่มต้นพจนานุกรมรายชื่อถนนหลัก (อังกฤษ = ไทย)
DEFAULT_MAIN_ROADS_DICT = {
    "New Petchaburi": "เพชรบุรีตัดใหม่",
    "Sukhumvit": "สุขุมวิท",
    "Phaholyothin": "พหลโยธิน",
    "Petchaburi": "เพชรบุรี",
    "Phetchaburi Rd": "เพชรบุรี",
    "Phetburi": "เพชรบุรี",
    "Phra Nakhon": "พระนคร",
    "Rama 1": "พระรามที่ 1",
    "Rama 2": "พระรามที่ 2",
    "Rama 3": "พระรามที่ 3",
    "Rama 4": "พระรามที่ 4",
    "Rama 5": "พระรามที่ 5",
    "Rama 6": "พระรามที่ 6",
    "Rama 7": "พระรามที่ 7",
    "Rama 8": "พระรามที่ 8",
    "Rama 9": "พระรามที่ 9",
    "Sathon": "สาทร",
    "Silom": "สีลม",
    "North Sathon Road": "สาทรเหนือ",
    "Charoennakorn Road": "เจริญนคร",
    "Surawong": "สุรวงศ์",
    "Ratchadamnoen Klang": "ราชดำเนินกลาง",
    "Ratchadamnoen Nok": "ราชดำเนินนอก",
    "Ratchadamnoen Nai": "ราชดำเนินใน",
    "Charoen Krung": "เจริญกรุง",
    "Bamrung Mueang": "บำรุงเมือง",
    "Din So": "ดินสอ",
    "Tanao": "ตะนาว",
    "Khaosan": "ข้าวสาร",
    "Phra Sumen": "พระสุเมรุ",
    "Samsen": "สามเสน",
    "Witthayu": "วิทยุ",
    "Lang Suan": "หลังสวน",
    "Chit Lom": "ชิดลม",
    "Ploenchit": "เพลินจิต",
    "Ratchadamri": "ราชดำริ",
    "Henri Dunant": "อังรีดูนังต์",
    "Phaya Thai": "พญาไท",
    "Banthat Thong": "บรรทัดทอง",
    "Chan": "จันทน์",
    "Sathu Pradit": "สาธุประดิษฐ์",
    "Nang Linchi": "นางลิ้นจี่",
    "Chuea Phloeng": "เชื้อเพลิง",
    "Naradhiwas Rajanagarindra": "นราธิวาสราชนครินทร์",
    "Ratchadaphisek": "รัชดาภิเษก",
    "Asok Montri": "อโศกมนตรี",
    "Thong Lo": "ทองหล่อ",
    "Ekkamai": "เอกมัย",
    "Pridi Banomyong": "ปรีดี พนมยงค์",
    "On Nut": "อ่อนนุช",
    "Bangna-Trat": "บางนา-ตราด",
    "Srinakarin": "ศรีนครินทร์",
    "Phatthanakan": "พัฒนาการ",
    "Ramkhamhaeng": "รามคำแหง",
    "Lat Phrao": "ลาดพร้าว",
    "Pradit Manutham": "ประดิษฐ์มนูธรรม",
    "Ram Inthra": "รามอินทรา",
    "Chaeng Watthana": "แจ้งวัฒนะ",
    "Ngam Wong Wan": "งามวงศ์วาน",
    "Tiwanon": "ติวานนท์",
    "Prachachuen": "ประชาชื่น",
    "Kamphaeng Phet": "กำแพงเพชร",
    "Vibhavadi Rangsit": "วิภาวดีรังสิต",
    "Sutthisan Winitchai": "สุทธิสารวินิจฉัย",
    "Pracha Uthit": "ประชาอุทิศ",
    "Phutthamonthon Sai 1": "พุทธมณฑลสาย 1",
    "Phutthamonthon Sai 2": "พุทธมณฑลสาย 2",
    "Phutthamonthon Sai 3": "พุทธมณฑลสาย 3",
    "Phutthamonthon Sai 4": "พุทธมณฑลสาย 4",
    "Borommaratchachonnani": "บรมราชชนนี",
    "Charan Sanitwong": "จรัญสนิทวงศ์",
    "Arun Amarin": "อรุณอมรินทร์",
    "Itsaraphap": "อิสรภาพ",
    "Prachathipok": "ประชาธิปก",
    "Somdej Phra Chao Tak Sin": "สมเด็จพระเจ้าตากสิน",
    "Charoen Nakhon": "เจริญนคร",
    "Rat Burana": "ราษฎร์บูรณะ",
    "Suksawat": "สุขสวัสดิ์",
    "Ekkachai": "เอกชัย",
    "Bang Khun Thian-Cha Thale": "บางขุนเทียน-ชายทะเล",
    "Kanchanaphisek": "กาญจนาภิเษก",
    "Outer Ring Road": "วงแหวนรอบนอก",
    "Phrannok": "พรานนก"
}

# ค่าเริ่มต้นพจนานุกรมรายชื่อย่านสำคัญ
DEFAULT_MAJOR_AREAS_DICT = {
    "Sukhumvit Road": "ถนนสุขุมวิท",
    "Khlong San": "คลองสาน",
    "Phahonyothin": "ถนนพหลโยธิน",
    "Phetchaburi": "ถนนเพชรบุรี",
    "Phahonyothin Road": "ถนนพหลโยธิน",
    "Phetchaburi Road": "ถนนเพชรบุรี",
    "Rama 1 Road": "ถนนพระรามที่ 1",
    "Rama 2 Road": "ถนนพระรามที่ 2",
    "Rama 3 Road": "ถนนพระรามที่ 3",
    "Rama 4 Road": "ถนนพระรามที่ 4",
    "Silom Road": "ถนนสีลม",
    "Sathorn Road": "ถนนสาทร",
    "Asok": "อโศก",
    "Thong Lo": "ทองหล่อ",
    "Ekkamai": "เอกมัย",
    "Siam": "สยาม",
    "Pratunam": "ประตูน้ำ",
    "Khaosan": "ข้าวสาร",
    "Silom": "สีลม",
    "Sathorn": "สาทร"
}

# Dictionary สำหรับ Map จุดรับ จุดส่ง ขนาดรถ และราคาตามกฎ
CAR_PRICING_MAP = {
    "5 SEAT": {"code": "5S", "price": "380"},
    "5S": {"code": "5S", "price": "380"},
    "7 SEAT": {"code": "7S", "price": "480"},
    "7S": {"code": "7S", "price": "480"},
    "CAM/7S": {"code": "Cam/7S", "price": "480"},
    "CAMRY/7S": {"code": "Cam/7S", "price": "480"},
    "CAMRY OR 7SEAT": {"code": "Cam/7S", "price": "480"}
}


# ====================== การตั้งค่า (เก็บใน Supabase) ======================
_settings_cache = None
_settings_lock = threading.RLock()
_settings_retry_at = 0
_settings_error = ""


def _with_defaults(data):
    data = dict(data or {})
    data.setdefault("wait_seconds", 10)
    data.setdefault("line_groups", [])
    data.setdefault("custom_keywords", [])
    data.setdefault("main_roads_dict", copy.deepcopy(DEFAULT_MAIN_ROADS_DICT))
    data.setdefault("major_areas_dict", copy.deepcopy(DEFAULT_MAJOR_AREAS_DICT))
    data.setdefault("custom_locations", {"metropole": "เพชรบุรีตัดใหม่", "c u inn": "จตุจักร"})
    data.pop("waiting_time", None)
    try:
        data["wait_seconds"] = int(data["wait_seconds"])
    except Exception:
        data["wait_seconds"] = 10
    return data


def load_settings(force=False):
    """อ่านการตั้งค่า (เก็บไว้ในหน่วยความจำ ไม่ต้องดึงจาก Supabase ทุกครั้ง)"""
    global _settings_cache, _settings_retry_at, _settings_error
    with _settings_lock:
        if _settings_cache is not None and not force:
            return _settings_cache
        if not force and time.time() < _settings_retry_at:
            return _with_defaults({})
        try:
            data = storage.get("settings")
            if data is None:
                # ครั้งแรก: ย้ายค่าจาก settings.json เดิม (ถ้ามี) ขึ้น Supabase
                data = _with_defaults(storage._read_legacy_settings() or {})
                try:
                    storage.set("settings", data)
                except storage.StorageError as e:
                    print(f"⚠️ บันทึกการตั้งค่าเริ่มต้นไม่สำเร็จ: {e}")
            _settings_cache = _with_defaults(data)
            _settings_error = ""
            return _settings_cache
        except storage.StorageError as e:
            _settings_error = str(e)
            _settings_retry_at = time.time() + 30
            print(f"❌ โหลดการตั้งค่าไม่สำเร็จ (ใช้ค่าเริ่มต้นชั่วคราว): {e}")
            return _with_defaults({})


def save_settings(data):
    """บันทึกการตั้งค่า คืน (สำเร็จไหม, ข้อความ)"""
    global _settings_cache
    with _settings_lock:
        current = copy.deepcopy(load_settings(force=True))
        if _settings_error:
            return False, f"บันทึกไม่สำเร็จ: เชื่อมต่อที่เก็บข้อมูลไม่ได้ ({_settings_error})"

        if "waiting_time" in data:
            try:
                val = float(data["waiting_time"])
                current["wait_seconds"] = int(val if val >= 5 else val * 60)
            except Exception:
                pass

        for key in ("line_groups", "custom_keywords", "main_roads_dict",
                    "major_areas_dict", "custom_locations"):
            if key in data:
                current[key] = data[key]

        try:
            storage.set("settings", current)
        except storage.StorageError as e:
            return False, f"บันทึกไม่สำเร็จ: {e}"
        _settings_cache = current
        return True, "บันทึกการตั้งค่าเรียบร้อยแล้ว"


# ====================== บันทึก/โหลด คิวใบงาน ======================
def persist_state():
    """บันทึกคิว + ประวัติใบงานลง Supabase (กันข้อมูลหายตอนเซิร์ฟเวอร์หลับ)"""
    with state_lock:
        snapshot = {
            "job_queue": list(job_queue),
            "latest_jobs": list(latest_jobs),
            "last_sender_id": last_sender_id,
            "timer_start_time": timer_start_time,
        }
    try:
        storage.set("job_state", snapshot)
    except storage.StorageError as e:
        print(f"⚠️ บันทึกคิวใบงานไม่สำเร็จ: {e}")


def restore_state():
    """โหลดคิวที่ค้างอยู่กลับมา และเริ่มนับเวลาต่อ"""
    global job_queue, latest_jobs, last_sender_id, timer_start_time
    try:
        snap = storage.get("job_state")
    except storage.StorageError as e:
        print(f"⚠️ โหลดคิวใบงานไม่สำเร็จ: {e}")
        return
    if not snap:
        return
    with state_lock:
        job_queue = snap.get("job_queue") or []
        latest_jobs = snap.get("latest_jobs") or []
        last_sender_id = snap.get("last_sender_id")
        if job_queue:
            wait_seconds = int(load_settings().get("wait_seconds", 10))
            started = snap.get("timer_start_time") or time.time()
            remaining = max(5, wait_seconds - (time.time() - started))
            _start_timer(remaining, started)
            print(f"♻️ กู้คืนคิวค้าง {len(job_queue)} ใบ จะส่งในอีก {int(remaining)} วินาที")


def _start_timer(seconds, started_at=None):
    global timer_thread, timer_start_time
    timer_start_time = started_at or time.time()
    timer_thread = threading.Timer(seconds, process_batch_jobs)
    timer_thread.daemon = True
    timer_thread.start()


# ====================== แปลงชื่อสถานที่ ======================
def _contains(text_lower, word):
    """เช็คคำ: ภาษาอังกฤษต้องเป็นคำเต็ม (กัน 'Chan' ไปตรงกับ 'Chang Klan'), ภาษาไทยเช็คแบบมีอยู่ในข้อความ"""
    w = str(word).strip().lower()
    if not w:
        return False
    if re.search(r"[a-z0-9]", w):
        return re.search(r"(?<![a-z0-9])" + re.escape(w) + r"(?![a-z0-9])", text_lower) is not None
    return w in text_lower


def _dict_candidates(d):
    """แปลงพจนานุกรม (dict หรือ list) เป็นรายการ (คำที่ใช้ค้นหา, ผลลัพธ์ภาษาไทย)"""
    out = []
    if isinstance(d, dict):
        for eng, th in d.items():
            eng_items = eng if isinstance(eng, list) else [eng]
            th_items = th if isinstance(th, list) else [th]
            th_items = [t for t in th_items if isinstance(t, str) and t.strip()]
            eng_items = [e for e in eng_items if isinstance(e, str) and e.strip()]
            result = th_items[0].strip() if th_items else (eng_items[0].strip() if eng_items else "")
            if not result:
                continue
            for word in eng_items + th_items:
                out.append((word, result))
    elif isinstance(d, list):
        for item in d:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key", "")).strip()
            aliases = item.get("aliases", [])
            alias_list = aliases if isinstance(aliases, list) else [aliases]
            if not key:
                continue
            for word in [key] + [a for a in alias_list if isinstance(a, str)]:
                out.append((word, key))
    # คำยาวก่อน เพื่อให้ "New Petchaburi" ชนะ "Petchaburi"
    out.sort(key=lambda x: len(str(x[0])), reverse=True)
    return out


def _match_dict(text_lower, d):
    for word, result in _dict_candidates(d):
        if _contains(text_lower, word):
            return result
    return ""


def parse_location_rule_based(raw_location):
    """แปลงจุดรับ-จุดส่งด้วยกฎ และดึงพจนานุกรมล่าสุดจาก Settings"""
    if not raw_location or raw_location == "-":
        return "-"

    cleaned = raw_location.strip()
    upper_loc = cleaned.upper()
    lower_loc = cleaned.lower()

    settings = load_settings()
    current_main_roads = settings.get("main_roads_dict", DEFAULT_MAIN_ROADS_DICT)
    current_major_areas = settings.get("major_areas_dict", DEFAULT_MAJOR_AREAS_DICT)
    custom_keywords = settings.get("custom_keywords", [])

    # 0. Custom Keywords ที่ผู้ใช้ตั้งค่าเพิ่มเอง
    for item in custom_keywords:
        kw = str(item.get("keyword", "")).strip()
        target = str(item.get("zone", "")).strip()
        if kw and kw.lower() in lower_loc:
            return target if target else cleaned

    # 0.1 เศษข้อความหลุด เช่น "ei Nuea" จาก Khlong Toei Nuea ให้ตีเป็นสุขุมวิท
    if "EI NUEA" in upper_loc or "KHLONG TOEI" in upper_loc:
        return "สุขุมวิท"

    # 1. สนามบินดอนเมือง
    if any(k in upper_loc for k in ["DMK", "DON MUEANG", "ดอนเมือง", "แอร์ดอน"]):
        return "แอร์ดอน"

    # 2. สนามบินสุวรรณภูมิ
    if any(k in upper_loc for k in ["BKK", "SUVARNABHUMI", "SVB", "แอร์สุ", "สุวรรณภูมิ"]):
        return "แอร์สุ"

    # 3. Sukhumvit ตามด้วยเลขซอย
    if re.search(r'\bSukhumvit\s*\d+\b', cleaned, flags=re.IGNORECASE):
        return "สุขุมวิท"

    # 4. ถนนหลัก แล้วค่อย 5. ย่านสำคัญ
    matched = _match_dict(lower_loc, current_main_roads) or _match_dict(lower_loc, current_major_areas)
    if matched:
        return matched

    # ไม่ตรงอะไรเลย ตัด "ซอย [ตัวเลข]" ออก
    cleaned_no_soi = re.sub(r'(?:ซอย|soi)\s*\d+', '', cleaned, flags=re.IGNORECASE).strip()
    return cleaned_no_soi if cleaned_no_soi else cleaned


def parse_job_line(line_text):
    # ป้องกันเคสที่ชื่อสถานที่/โรงแรมมีเครื่องหมาย - อยู่ข้างใน (เช่น Manhattan Hotel Bangkok-สุขุมวิท หรืออื่นๆ)
    # ให้มองหาเครื่องหมาย - ตัวสุดท้ายของบรรทัดเส้นทาง หรือเช็คเครื่องหมาย - ที่คั่นระหว่างจุดรับ-จุดส่งหลัก
    parts = line_text.split('-')
    
    # ถ้ามีมากกว่า 2 ส่วน (แสดงว่ามีเครื่องหมาย - เกินมาในชื่อสถานที่ เช่น Manhattan Hotel Bangkok-สุขุมวิท)
    if len(parts) > 2:
        # สมมติว่ารูปแบบคือ [จุดรับ] - [จุดส่ง] แต่จุดรับดันมีขีดคั่น ให้รวมตัวแรกกับตัวกลางเข้าด้วยกันเป็นจุดรับ
        pickup_raw = "-".join(parts[:-1]).strip()
        dropoff_raw = parts[-1].strip()
    elif len(parts) == 2:
        pickup_raw = parts[0].strip()
        dropoff_raw = parts[1].strip()
    else:
        return line_text, "-"
        
    pickup_clean = parse_location_rule_based(pickup_raw)
    dropoff_clean = parse_location_rule_based(dropoff_raw)
    
    return pickup_clean, dropoff_clean

def _place_code(mapped):
    """แปลงจุดรับ/จุดส่งเป็นรหัสย่อ: แอร์สุ=S, แอร์ดอน=D, อื่นๆ=BK"""
    if not mapped or mapped == "-":
        return "-"
    if mapped == "แอร์สุ":
        return "S"
    if mapped == "แอร์ดอน":
        return "D"
    return "BK"


def parse_job_text(raw_text, fallback_id="F01"):
    if not raw_text:
        return {}

    # รหัสใบงาน = หัวข้อบรรทัดแรกของใบงาน (เช่น F03) รหัสอื่นก็ใช้ได้หมด
    job_id = ""
    for ln in raw_text.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        # บรรทัดแรกต้องเป็นรหัสสั้นๆ ไม่ใช่บรรทัดข้อมูลที่ขึ้นต้นด้วย 【
        if "【" not in ln and "】" not in ln and len(ln) <= 20:
            job_id = ln
        break
    if not job_id:
        id_match = re.search(r'\b(F\d+)\b', raw_text, re.IGNORECASE)
        job_id = id_match.group(1).strip() if id_match else fallback_id

    date_match = re.search(r'(?:【(?:日期วันที่|日期|วันที่)】|วันที่|Date)[:\s]*([\d/\-]+)', raw_text, re.IGNORECASE)
    date_val = date_match.group(1).strip() if date_match else "-"

    time_match = re.search(r'(?:【(?:เวลาเวลา|เวลา|เวลา)】|เวลา|Time)[:\s]*([\d:]+)', raw_text, re.IGNORECASE)
    if not time_match:
        time_match = re.search(r'(\d{2}:\d{2})', raw_text)
    time_val = time_match.group(1).strip() if time_match else "-"

    flight_val = "-"
    flight_match = re.search(r'(?:【(?:航班flight|航班|flight)】|เที่ยวบิน|flight|Flight)[:\s]*([A-Za-z0-9]+)', raw_text, re.IGNORECASE)
    if not flight_match:
        flight_match = re.search(r'✈️?\s*([A-Za-z]{2}\d+|\d{3,4})', raw_text)
    if flight_match:
        flight_val = flight_match.group(1).strip()

    # ดึงค่าจากแท็ก 【接รับ】 และ 【ส่ง】 โดยตรง 100% ตามรูปแบบในรูปภาพ
    pickup_raw = "-"
    dropoff_raw = "-"

    pickup_tag = re.search(r'【接รับ】\s*(.+)', raw_text)
    if pickup_tag:
        pickup_raw = pickup_tag.group(1).strip()

    dropoff_tag = re.search(r'【送ส่ง】\s*(.+)', raw_text)
    if dropoff_tag:
        dropoff_raw = dropoff_tag.group(1).strip()

    # แปลงชื่อสถานที่
    pickup_mapped = parse_location_rule_based(pickup_raw)
    dropoff_mapped = parse_location_rule_based(dropoff_raw)

    # จัดการเรื่องขนาดรถและราคา
    car_raw_match = re.search(r'(?:【(?:车型ขนาดรถ|车型|ขนาดรถ)】|รถ|ขนาดรถ|Car)[:\s]*(.+)', raw_text, re.IGNORECASE)
    if car_raw_match:
        car_raw = car_raw_match.group(1).strip().upper()
    else:
        car_raw = "5 SEAT"
        for key in CAR_PRICING_MAP.keys():
            if key in raw_text.upper():
                car_raw = key
                break
    
    car_info = CAR_PRICING_MAP.get(car_raw)
    if not car_info:
        # เช่น "7 SEAT (CAMRY)" -> หาคำที่ยาวที่สุดที่อยู่ในข้อความ
        for key in sorted(CAR_PRICING_MAP.keys(), key=len, reverse=True):
            if key in car_raw:
                car_info = CAR_PRICING_MAP[key]
                break
    if not car_info:
        car_info = {"code": "5S", "price": "380"}
    car_code = car_info["code"]
    price_val = car_info["price"]

    order_match = re.search(r'(?:【(?:客户订单号|订单号|Order)】|Order|Order Number|คำสั่งซื้อ|Order ID)[:\s]*([0-9A-Za-z_-]+)', raw_text, re.IGNORECASE)
    if not order_match:
        order_match = re.search(r'\b(\d{8,20})\b', raw_text)
    order_val = order_match.group(1).strip() if order_match else "-"

    # เงื่อนไขไอคอน: ถ้าตำแหน่งที่ 2 (จุดส่ง) เป็น แอร์ดอน หรือ แอร์สุ ให้ใช้ 🔥 ที่เหลือใช้ 🥶
    if dropoff_mapped in ["แอร์ดอน", "แอร์สุ"]:
        icon_symbol = "🔥"
    else:
        icon_symbol = "🥶"

    # รหัสจุดรับ-จุดส่ง: S = แอร์สุ, D = แอร์ดอน, BK = สถานที่/โรงแรมในกรุงเทพ
    route_code = f"{_place_code(pickup_mapped)}-{_place_code(dropoff_mapped)}"

    formatted_summary = f"{job_id}({icon_symbol}){time_val}/{price_val}#{car_code}/{route_code}\n{pickup_mapped}-{dropoff_mapped} ✈️{flight_val}\n{order_val}"

    return {
        "id": job_id,
        "date": date_val,
        "time": time_val,
        "pickup_raw": pickup_raw,
        "pickup": pickup_mapped,
        "dropoff_raw": dropoff_raw,
        "dropoff": dropoff_mapped,
        "flight": flight_val,
        "car_code": car_code,
        "price": price_val,
        "order": order_val,
        "formatted_summary": formatted_summary
    }


def _job_key(job):
    order = str(job.get("order", "")).strip()
    if order and order != "-":
        return "order:" + order
    return "text:" + str(job.get("text", "")).strip()


def add_job_to_queue(text, sender_id=None):
    """เพิ่มใบงานเข้าคิว คืน (สำเร็จไหม, ข้อมูลใบงาน หรือ ข้อความ)"""
    global latest_jobs, last_sender_id

    with state_lock:
        # รหัสใบงานตามลำดับในคิวปัจจุบัน (F01, F02, ...) กัน F ชนกัน
        used_ids = {j.get("id") for j in job_queue}
        seq = len(job_queue) + 1
        while f"F{seq:02d}" in used_ids:
            seq += 1
        parsed_info = parse_job_text(text, fallback_id=f"F{seq:02d}")

        now = now_th()
        job_item = {
            "id": parsed_info["id"],
            "date": parsed_info["date"] if parsed_info["date"] != "-" else now.strftime("%d/%m/%Y"),
            "text": text,
            "raw_text": text,
            "time": parsed_info["time"] if parsed_info["time"] != "-" else now.strftime("%H:%M:%S"),
            "pickup": parsed_info["pickup"],
            "pickup_display": parsed_info["pickup"],
            "dropoff": parsed_info["dropoff"],
            "dropoff_display": parsed_info["dropoff"],
            "flight": parsed_info["flight"],
            "order": parsed_info["order"],
            "car_code": parsed_info["car_code"],
            "price": parsed_info["price"],
            "formatted_summary": parsed_info["formatted_summary"],
            "received_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "รอส่ง"
        }

        # กันใบงานซ้ำ (เช่น LINE ส่ง webhook ซ้ำ หรือกดส่งซ้ำ)
        key = _job_key(job_item)
        if any(_job_key(j) == key for j in job_queue):
            return False, "พบใบงานซ้ำในคิว (Order เดียวกัน)"

        job_queue.append(job_item)
        if sender_id:
            last_sender_id = sender_id

        latest_jobs.insert(0, job_item)
        latest_jobs = latest_jobs[:MAX_HISTORY]

        if len(job_queue) == 1 and timer_thread is None:
            wait_seconds = int(load_settings().get("wait_seconds", 10))
            _start_timer(wait_seconds)
            print(f"⏱️ เริ่มนับเวลาประมวลผลอีก {wait_seconds} วินาที...")

    persist_state()
    return True, job_item


def _date_sort_key(d):
    for fmt in ("%d/%m/%Y", "%Y/%m/%d", "%d/%m/%y"):
        try:
            return (0, datetime.datetime.strptime(d, fmt))
        except Exception:
            pass
    return (1, datetime.datetime.max)


def _id_sort_key(job):
    m = re.search(r'(\d+)', str(job.get("id", "")))
    return (int(m.group(1)) if m else 10**9, str(job.get("id", "")))


def generate_batch_summary(jobs):
    if not jobs:
        return ""

    date_groups = {}
    for job in jobs:
        raw_date = str(job.get("date") or now_th().strftime("%d/%m/%Y"))
        try:
            job_date = datetime.datetime.strptime(raw_date.replace('-', '/'), "%d/%m/%Y").strftime("%d/%m/%Y")
        except Exception:
            job_date = raw_date
        date_groups.setdefault(job_date, []).append(job)

    blocks = []
    sorted_dates = sorted(date_groups.keys(), key=_date_sort_key)

    for d in sorted_dates:
        blocks.append(f"📅 {d}")
        blocks.append("")
        sorted_jobs = sorted(date_groups[d], key=_id_sort_key)
        for i, job in enumerate(sorted_jobs):
            blocks.append(job["formatted_summary"])
            if i < len(sorted_jobs) - 1:
                blocks.append("")
        if d != sorted_dates[-1]:
            blocks.append("")

    return "\n".join(blocks)


def process_batch_jobs():
    """ส่งสรุปใบงานทั้งหมดในคิว: LINE ก่อน แล้วค่อยบันทึก Google Sheets"""
    global job_queue, timer_thread, timer_start_time, last_sender_id

    with state_lock:
        if timer_thread is not None and threading.current_thread() is not timer_thread:
            timer_thread.cancel()
        jobs = list(job_queue)
        target_sender_id = last_sender_id
        # เคลียร์คิวทันที ใบงานใหม่ที่เข้ามาระหว่างส่งจะเริ่มรอบใหม่ได้เลย
        job_queue = []
        timer_thread = None
        timer_start_time = None
        last_sender_id = None

    if not jobs:
        persist_state()
        return

    try:
        summary_text = generate_batch_summary(jobs)
        if summary_text:
            settings = load_settings()
            active_groups = [g for g in settings.get("line_groups", []) if g.get("group_id")]
            targets = []
            if target_sender_id:
                targets.append(("ผู้ส่ง (LINE OA)", target_sender_id))
            for grp in active_groups:
                targets.append((f"กลุ่ม {grp.get('name')}", grp.get("group_id")))

            # LINE จำกัด 5000 ตัวอักษรต่อข้อความ
            chunks = [summary_text[i:i + 4900] for i in range(0, len(summary_text), 4900)][:5]
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                for label, to in targets:
                    try:
                        line_bot_api.push_message(PushMessageRequest(
                            to=to, messages=[TextMessage(text=c) for c in chunks]
                        ))
                    except Exception as e:
                        print(f"❌ ส่งข้อความไปยัง {label} ล้มเหลว: {e}")

            if GOOGLE_SPREADSHEET_ID:
                try:
                    ok = save_to_google_sheets(GOOGLE_SPREADSHEET_ID, [j["text"] for j in jobs], jobs)
                    print("✅ บันทึกข้อมูลลง Google Sheets สำเร็จ" if ok else "❌ บันทึก Google Sheets ล้มเหลว")
                except Exception as e:
                    print(f"❌ บันทึก Google Sheets ล้มเหลว: {e}")
    except Exception as e:
        print(f"❌ ประมวลผลคิวใบงานผิดพลาด: {e}")
    finally:
        sent_keys = {(_job_key(j), j.get("id")) for j in jobs}
        with state_lock:
            for job in latest_jobs:
                if (_job_key(job), job.get("id")) in sent_keys:
                    job["status"] = "ส่งแล้ว"
        persist_state()


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    received_text = event.message.text.strip()
    source_type = event.source.type
    user_id = getattr(event.source, 'user_id', None)

    if source_type == 'group' and received_text.lower() == "id":
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text=f"{event.source.group_id}")]
                )
            )
        return

    if "【客户订单号】" not in received_text:
        return

    add_job_to_queue(received_text, sender_id=user_id)


# ====================== หน้าเว็บ / API ======================
@app.route("/")
def index():
    return render_template("ui.html")


@app.route("/health")
def health():
    return "OK", 200


@app.route("/api/status", methods=["GET"])
def api_status():
    settings = load_settings()
    wait_seconds = int(settings.get("wait_seconds", 10))

    with state_lock:
        time_left = 0
        if timer_start_time:
            time_left = max(0, int(wait_seconds - (time.time() - timer_start_time)))
        queue_count = len(job_queue)
        jobs_copy = list(latest_jobs)

    return jsonify({
        "line_status": bool(LINE_CHANNEL_ACCESS_TOKEN and LINE_CHANNEL_SECRET),
        "sheets_status": bool(GOOGLE_SPREADSHEET_ID and has_credentials()),
        "storage_ok": not _settings_error,
        "storage_mode": "supabase" if storage.using_supabase() else "local",
        "queue_count": queue_count,
        "time_left_seconds": time_left,
        "max_wait_seconds": wait_seconds,
        "latest_jobs": jobs_copy,
        "custom_keywords": settings.get("custom_keywords", []),
        "line_groups": settings.get("line_groups", []),
        "main_roads_dict": settings.get("main_roads_dict", DEFAULT_MAIN_ROADS_DICT),
        "major_areas_dict": settings.get("major_areas_dict", DEFAULT_MAJOR_AREAS_DICT),
        "settings": settings
    })


_sheets_health_cache = {"at": 0, "ok": False}


@app.route("/api/system_health", methods=["GET"])
def api_system_health():
    line_connected = bool(LINE_CHANNEL_ACCESS_TOKEN and LINE_CHANNEL_SECRET)

    # เช็ค Google Sheets ไม่เกินทุก 5 นาที (กันโดนจำกัดการใช้งาน)
    if GOOGLE_SPREADSHEET_ID and time.time() - _sheets_health_cache["at"] > 300:
        ok = False
        try:
            client = get_sheet_client()
            if client:
                client.open_by_key(GOOGLE_SPREADSHEET_ID)
                ok = True
        except Exception as e:
            print(f"Google Sheets check error: {e}")
        _sheets_health_cache.update(at=time.time(), ok=ok)

    storage_ok, storage_msg = storage.health()

    return jsonify({
        "line_bot": line_connected,
        "google_sheets": bool(GOOGLE_SPREADSHEET_ID) and _sheets_health_cache["ok"],
        "storage": storage_ok,
        "storage_message": storage_msg,
    })


@app.route("/callback", methods=['GET', 'POST'])
def callback():
    if request.method == 'GET':
        return 'OK', 200

    signature = request.headers.get('X-Line-Signature', '')
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'


@app.route("/api/get_sheets_data", methods=["GET"])
def api_get_sheets_data():
    client = get_sheet_client()
    if not client or not GOOGLE_SPREADSHEET_ID:
        return jsonify({"success": False, "message": "Google Sheets not connected"}), 400

    try:
        spreadsheet = client.open_by_key(GOOGLE_SPREADSHEET_ID)
        rows = spreadsheet.worksheet("SUMMARY").get_all_records()
        return jsonify({"success": True, "data": rows})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/add_job", methods=["POST"])
def api_add_job():
    data = request.get_json(silent=True) or {}
    text = str(data.get("text", "")).strip()
    if not text:
        return jsonify({"success": False, "message": "ข้อความว่างเปล่า"}), 400
    ok, result = add_job_to_queue(text)
    if ok:
        return jsonify({"success": True, "id": result["id"], "message": "เพิ่มใบงานสำเร็จ"})
    return jsonify({"success": False, "message": result})


@app.route("/api/save_settings", methods=["POST"])
def api_save_settings():
    new_settings = request.get_json(silent=True) or {}
    ok, msg = save_settings(new_settings)
    return jsonify({"success": ok, "message": msg}), (200 if ok else 500)


@app.route("/api/trigger_send", methods=["POST"])
def api_trigger_send():
    process_batch_jobs()
    return jsonify({"success": True, "message": "ส่งสรุปใบงานเรียบร้อยแล้ว"})


@app.route('/api/delete_job', methods=['POST'])
@app.route("/api/delete_job/<job_id>", methods=["DELETE", "POST"])
def api_delete_job(job_id=None):
    try:
        if not job_id:
            data = request.get_json(silent=True) or {}
            job_id = data.get('id')

        if not job_id:
            return jsonify({"success": False, "message": "ไม่พบรหัสใบงานที่ต้องการลบ"}), 400

        if not GOOGLE_SPREADSHEET_ID:
            return jsonify({"success": False, "message": "ยังไม่ได้ตั้งค่า GOOGLE_SPREADSHEET_ID"}), 400

        if delete_row_from_google_sheets(GOOGLE_SPREADSHEET_ID, job_id):
            return jsonify({"success": True, "message": f"ลบใบงาน {job_id} สำเร็จ"})
        return jsonify({"success": False, "message": "ไม่พบข้อมูลใน Google Sheets หรือเกิดข้อผิดพลาด"}), 404

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/clear_queue", methods=["POST"])
def api_clear_queue():
    global job_queue, timer_thread, timer_start_time
    with state_lock:
        if timer_thread:
            timer_thread.cancel()
        num_cleared = len(job_queue)
        cleared_keys = {_job_key(j) for j in job_queue}
        job_queue = []
        timer_thread = None
        timer_start_time = None
        for job in latest_jobs:
            if job.get("status") == "รอส่ง" and _job_key(job) in cleared_keys:
                job["status"] = "ยกเลิก"
    persist_state()
    return jsonify({"success": True, "num_cleared": num_cleared, "message": "ล้าง Queue เรียบร้อยแล้ว"})


# ====================== กันเซิร์ฟเวอร์หลับ (Render Free) ======================
def _keep_alive_loop(url):
    while True:
        time.sleep(10 * 60)
        try:
            requests.get(url, timeout=15)
        except Exception as e:
            print(f"⚠️ keep-alive ping ล้มเหลว: {e}")


def _start_background():
    load_settings()
    restore_state()
    base = os.getenv("KEEP_ALIVE_URL") or os.getenv("RENDER_EXTERNAL_URL")
    if base and os.getenv("KEEP_ALIVE", "1") != "0":
        url = base.rstrip("/") + "/health"
        threading.Thread(target=_keep_alive_loop, args=(url,), daemon=True).start()
        print(f"🔔 เปิด keep-alive ping ไปที่ {url} ทุก 10 นาที")
    print(f"💾 ที่เก็บข้อมูล: {'Supabase' if storage.using_supabase() else 'ไฟล์ในเครื่อง (local_store.json)'}")


_start_background()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=False)
