import os
import re
import json
import time
import threading
import datetime
from flask import Flask, request, abort, render_template, jsonify, redirect, url_for
from linebot.v3.messaging import TextMessage, ReplyMessageRequest
from dotenv import load_dotenv

# Import LINE SDK (v3)
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, PushMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

# Import local modules
from ai import summarize_jobs_with_ai
from google_sheet import save_to_google_sheets, get_sheet_client, delete_row_from_google_sheets

load_dotenv()

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "")

handler = WebhookHandler(LINE_CHANNEL_SECRET)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)

# Global State
job_queue = []          
timer_thread = None     
timer_start_time = None 
latest_jobs = []        
last_sender_id = None   

# Dictionary สำหรับ Map จุดรับ จุดส่ง ขนาดรถ และราคาตามกฎ
PICKUP_MAP = {
    "BKK": "แอร์สุ",
    "BKK T1": "แอร์สุ",
    "DMK": "แอร์ดอน",
    "DMK T1": "แอร์ดอน"
}

CAR_PRICING_MAP = {
    "5 SEAT": {"code": "5S", "price": "380"},
    "5S": {"code": "5S", "price": "380"},
    "7 SEAT": {"code": "7S", "price": "480"},
    "7S": {"code": "7S", "price": "480"},
    "CAM/7S": {"code": "7S", "price": "480"},
    "CAMRY/7S": {"code": "7S", "price": "480"}
}

def load_settings():
    default_settings = {
        "wait_seconds": 120,
        "line_groups": [],
        "custom_keywords": [],
        "custom_locations": {
            "metropole": "เพชรบุรีตัดใหม่",
            "c u inn": "จตุจักร"
        }
    }
    if os.path.exists("settings.json"):
        with open("settings.json", "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                if "wait_seconds" not in data:
                    data["wait_seconds"] = 120
                if "line_groups" not in data:
                    data["line_groups"] = []
                if "custom_keywords" not in data:
                    data["custom_keywords"] = []
                if "custom_locations" not in data:
                    data["custom_locations"] = default_settings["custom_locations"]
                return data
            except:
                pass
    return default_settings

def save_settings(data):
    current = load_settings()
                
    if "waiting_time" in data:
        try:
            val = float(data["waiting_time"])
            current["wait_seconds"] = int(val if val >= 30 else val * 60)
        except:
            pass
            
    if "line_groups" in data:
        current["line_groups"] = data["line_groups"]

    if "custom_keywords" in data:
        current["custom_keywords"] = data["custom_keywords"]

    if "custom_locations" in data:
        current["custom_locations"] = data["custom_locations"]
    
    with open("settings.json", "w", encoding="utf-8") as f:
        json.dump(current, f, ensure_ascii=False, indent=2)

def map_dropoff_location(raw_dropoff):
    """แปลงจุดส่งให้ชาญฉลาดขึ้น ค้นหาถนนและย่านสำคัญจากทั้งข้อความ"""
    if not raw_dropoff or raw_dropoff == "-":
        return "-"
    
    text = raw_dropoff.strip()
    lower_text = text.lower()
    lower_text = re.sub(r'\s+', ' ', lower_text)

    # 0. ตรวจสอบ Custom Keywords จาก Settings
    settings = load_settings()
    custom_keywords = settings.get("custom_keywords", [])
    for item in custom_keywords:
        kw = item.get("keyword", "").strip().lower()
        zone = item.get("zone", "").strip()
        if kw and kw in lower_text:
            return zone

    # ตรวจสอบ Custom Locations แบบเดิม
    custom_locs = settings.get("custom_locations", {})
    for keyword, mapped_name in custom_locs.items():
        if keyword.lower() in lower_text:
            return mapped_name

    # 1. ตรวจจับถนนหลักหรือ New Petchaburi เป็นอันดับแรก
    if any(k in lower_text for k in ["new phetchaburi", "new petchaburi", "เพชรบุรีตัดใหม่"]):
        return "เพชรบุรีตัดใหม่"

    # 2. ตรวจจับ “ซอยที่มีเลข”
    soi_patterns = [
        { "regex": r'(?:sukhumvit|สุขุมวิท).*(?:soi|ซอย)\s*(\d+)', "template": "สุขุมวิท $1" },
        { "regex": r'(?:soi|ซอย)\s*(\d+).*(?:sukhumvit|สุขุมวิท)', "template": "สุขุมวิท $1" },
        { "regex": r'(?:sukhumvit|สุขุมวิท)\s*[-]?\s*(\d+)', "template": "สุขุมวิท $1" },
        { "regex": r'(?:phahonyothin|พหลโยธิน).*(?:soi|ซอย)\s*(\d+)', "template": "พหลโยธิน $1" },
        { "regex": r'(?:soi|ซอย)\s*(\d+).*(?:phahonyothin|พหลโยธิน)', "template": "พหลโยธิน $1" },
        { "regex": r'(?:phetchaburi|เพชรบุรี).*(?:soi|ซอย)\s*(\d+)', "template": "เพชรบุรี $1" },
        { "regex": r'(?:soi|ซอย)\s*(\d+).*(?:phetchaburi|เพชรบุรี)', "template": "เพชรบุรี $1" }
    ]

    for p in soi_patterns:
        match = re.search(p["regex"], lower_text)
        if match:
            return p["template"].replace("$1", match.group(1))

    # 3. ตรวจจับ “ย่านสำคัญ / แหล่งท่องเที่ยว / เขตพื้นที่”
    districts = [
        { "keywords": ["pratunam", "ประตูน้ำ"], "result": "ประตูน้ำ" },
        { "keywords": ["khao san", "khaosan", "ข้าวสาร"], "result": "ข้าวสาร" },
        { "keywords": ["yaowarat", "chinatown", "เยาวราช"], "result": "เยาวราช" },
        { "keywords": ["thonglor", "thong lor", "ทองหล่อ"], "result": "ทองหล่อ" },
        { "keywords": ["ekkamai", "เอกมัย"], "result": "เอกมัย" },
        { "keywords": ["ploenchit", "ploen chit", "เพลินจิต"], "result": "เพลินจิต" },
        { "keywords": ["asok", "อโศก"], "result": "อโศก" },
        { "keywords": ["prompong", "phrom phong", "พร้อมพงษ์"], "result": "พร้อมพงษ์" },
        { "keywords": ["ari", "aree", "อารีย์"], "result": "อารีย์" },
        { "keywords": ["victory monument", "อนุสาวรีย์"], "result": "อนุสาวรีย์" },
        { "keywords": ["rangsit", "klong 1", "คลอง 1", "รังสิต"], "result": "รังสิต" },
        { "keywords": ["ratchathewi", "ratchatevee", "ราชเทวี"], "result": "ราชเทวี" },
        { "keywords": ["pathum wan", "pathumwan", "ปทุมวัน"], "result": "ปทุมวัน" },
        { "keywords": ["huai khwang", "ห้วยขวาง"], "result": "ห้วยขวาง" },
        { "keywords": ["ratchada", "รัชดา"], "result": "รัชดา" },
        { "keywords": ["ratchayothin", "รัชโยธิน"], "result": "รัชโยธิน" },
        { "keywords": ["bang na", "bangna", "บางนา"], "result": "บางนา" },
        { "keywords": ["srinakarin", "srinagarind", "ศรีนครินทร์"], "result": "ศรีนครินทร์" },
        { "keywords": ["riverside", "charoenkrung", "เจริญกรุง"], "result": "เจริญกรุง" },
        { "keywords": ["siam"], "result": "สยาม" },
        { "keywords": ["kasem san", "เกษมสันต์"], "result": "เกษมสันต์" },
        { "keywords": ["chatuchak", "จตุจักร"], "result": "จตุจักร" },
        { "keywords": ["c u inn", "cu inn"], "result": "จตุจักร" }
    ]

    for d in districts:
        for kw in d["keywords"]:
            if kw in lower_text:
                return d["result"]

    # 4. ตรวจจับ “ถนนสายหลัก”
    main_roads = [
        { "keywords": ["witthayu", "wireless", "วิทยุ"], "result": "วิทยุ" },
        { "keywords": ["sathon", "sathorn", "สาทร"], "result": "สาทร" },
        { "keywords": ["silom", "สีลม"], "result": "สีลม" },
        { "keywords": ["rama 9", "rama ix", "พระราม 9"], "result": "พระราม 9" },
        { "keywords": ["rama 4", "rama iv", "พระราม 4"], "result": "พระราม 4" },
        { "keywords": ["ladprao", "lat phrao", "ลาดพร้าว"], "result": "ลาดพร้าว" },
        { "keywords": ["sukhumvit", "สุขุมวิท"], "result": "สุขุมวิท" },
        { "keywords": ["phetchaburi", "เพชรบุรี"], "result": "เพชรบุรี" },
        { "keywords": ["phahonyothin", "พหลโยธิน"], "result": "พหลโยธิน" },
        { "keywords": ["กำแพงเพชร", "kamphaeng phet"], "result": "จตุจักร" }
    ]

    for r in main_roads:
        for kw in r["keywords"]:
            if kw in lower_text:
                return r["result"]

    clean_text = text.split(',')[0].split('(')[0].strip()
    return clean_text if len(clean_text) < 15 else "จตุจักร"

def parse_job_text(raw_text, fallback_id="F01"):
    """แกะข้อมูลใบงาน รองรับจุดรับ จุดส่ง และแปลงค่าอัตโนมัติ"""
    if not raw_text:
        return {}

    lines = [line.strip() for line in raw_text.strip().split('\n') if line.strip()]
    
    # 1. รหัสใบงาน (Job ID)
    id_match = re.search(r'(?:รหัสใบงาน|Job ID|ID)[:\s]*([A-Za-z0-9_-]+)', raw_text, re.IGNORECASE)
    if not id_match and lines:
        id_match = re.search(r'^([A-Za-z0-9_-]+)', lines[0])
    job_id = id_match.group(1).strip() if id_match else fallback_id

    # 2. วันที่ (Date)
    date_match = re.search(r'(?:【(?:日期วันที่|日期|วันที่)】|วันที่|Date)[:\s]*([\d/\-]+)', raw_text, re.IGNORECASE)
    date_val = date_match.group(1).strip() if date_match else "-"

    # 3. เวลา (Time)
    time_match = re.search(r'(?:【(?:时间时间|时间|เวลา)】|เวลา|Time)[:\s]*([\d:]+)', raw_text, re.IGNORECASE)
    if not time_match:
        time_match = re.search(r'(\d{2}:\d{2})', raw_text)
    time_val = time_match.group(1).strip() if time_match else "-"

    # 4. เที่ยวบิน (Flight)
    flight_val = "-"
    flight_match = re.search(r'(?:【(?:航班flight|航班|flight)】|เที่ยวบิน|flight|Flight)[:\s]*([A-Za-z0-9]+)', raw_text, re.IGNORECASE)
    if not flight_match:
        flight_match = re.search(r'✈️?\s*([A-Za-z]{2}\d+|\d{3,4})', raw_text)
    if flight_match:
        flight_val = flight_match.group(1).strip()

    # 5. จุดรับ (Pickup)
    pickup_raw_match = re.search(r'(?:【(?:接รับ|接|รับ)】|จุดรับ|Pickup|From)[:\s]*(.+)', raw_text, re.IGNORECASE)
    if pickup_raw_match:
        pickup_raw = pickup_raw_match.group(1).strip()
    else:
        upper_text = raw_text.upper()
        if "DMK" in upper_text:
            pickup_raw = "DMK"
        elif "BKK" in upper_text:
            pickup_raw = "BKK"
        else:
            pickup_raw = "-"
            
    pickup_upper = pickup_raw.upper().strip()
    if any(k in pickup_upper for k in ["DMK", "DON MUEANG"]):
        pickup_mapped = "แอร์ดอน"
    elif any(k in pickup_upper for k in ["BKK", "SUVARNABHUMI", "SVB"]):
        pickup_mapped = "แอร์สุ"
    else:
        pickup_mapped = PICKUP_MAP.get(pickup_upper, pickup_raw)

    # 6. จุดส่ง (Dropoff)
    dropoff_raw = "-"
    dropoff_raw_match = re.search(r'(?:【(?:送ส่ง|ส่ง|ส่ง)】|จุดส่ง|Dropoff|Drop-off|To)[:\s]*(.+)', raw_text, re.IGNORECASE)
    if dropoff_raw_match:
        dropoff_raw = dropoff_raw_match.group(1).strip()
        dropoff_raw = re.sub(r'[\),].*$', '', dropoff_raw).strip()
    else:
        for line in lines:
            if any(k in line for k in ["【", "✈️", "รหัส", "Order", "380", "480"]) or ":" in line:
                continue
            if line != pickup_raw and line != time_val:
                dropoff_raw = line
                break

    dropoff_mapped = map_dropoff_location(dropoff_raw)

    # 7. ขนาดรถและราคา (Car & Price)
    car_raw_match = re.search(r'(?:【(?:车型ขนาดรถ|车型|ขนาดรถ)】|รถ|ขนาดรถ|Car)[:\s]*(.+)', raw_text, re.IGNORECASE)
    if car_raw_match:
        car_raw = car_raw_match.group(1).strip().upper()
    else:
        car_raw = "5 SEAT"
        for key in CAR_PRICING_MAP.keys():
            if key in raw_text.upper():
                car_raw = key
                break
    
    car_info = CAR_PRICING_MAP.get(car_raw, {"code": "5S", "price": "380"})
    car_code = car_info["code"]
    price_val = car_info["price"]

    # 8. หมายเลขคำสั่งซื้อ (Order)
    order_match = re.search(r'(?:【(?:客户订单号|订单号|Order)】|Order|Order Number|คำสั่งซื้อ|Order ID)[:\s]*([0-9A-Za-z_-]+)', raw_text, re.IGNORECASE)
    if not order_match:
        order_match = re.search(r'\b(\d{8,20})\b', raw_text)
    order_val = order_match.group(1).strip() if order_match else "-"

    formatted_summary = f"{job_id}(🥶){time_val}/{price_val}#{car_code}\n{pickup_mapped}-{dropoff_mapped} ✈️{flight_val}\n{order_val}"

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

def add_job_to_queue(text, sender_id=None):
    global job_queue, timer_thread, timer_start_time, latest_jobs, last_sender_id

    now_str = datetime.datetime.now().strftime("%H:%M:%S")
    fallback_id = f"F{len(latest_jobs) + 1:02d}"
    
    parsed_info = parse_job_text(text, fallback_id=fallback_id)

    job_item = {
        "id": parsed_info["id"],
        "date": parsed_info["date"] if parsed_info["date"] != "-" else datetime.datetime.now().strftime("%d/%m/%Y"),
        "text": text,           
        "raw_text": text,       
        "time": parsed_info["time"] if parsed_info["time"] != "-" else now_str,
        "pickup": parsed_info["pickup"],          
        "pickup_display": parsed_info["pickup"],    
        "dropoff": parsed_info["dropoff"],        
        "dropoff_display": parsed_info["dropoff"],  
        "flight": parsed_info["flight"],
        "order": parsed_info["order"],
        "car_code": parsed_info["car_code"],
        "price": parsed_info["price"],
        "formatted_summary": parsed_info["formatted_summary"],
        "status": "รอส่ง"
    }

    job_queue.append(job_item)
    if sender_id:
        last_sender_id = sender_id

    latest_jobs.insert(0, job_item)
    latest_jobs = latest_jobs[:30]

    if len(job_queue) == 1 and timer_thread is None:
        settings = load_settings()
        wait_seconds = int(settings.get("wait_seconds", 120))
        timer_start_time = time.time()
        timer_thread = threading.Timer(wait_seconds, process_batch_jobs)
        timer_thread.start()
        print(f"⏱️ เริ่มนับเวลาประมวลผลอีก {wait_seconds} วินาที...")
    return True

def generate_batch_summary():
    if not job_queue:
        return ""

    first_date = job_queue[0].get("date", datetime.datetime.now().strftime("%d/%m/%Y"))
    
    if len(job_queue) > 1:
        lines = [f"📅 {first_date}", ""]
        for i, job in enumerate(job_queue):
            lines.append(job["formatted_summary"])
            if i < len(job_queue) - 1:
                lines.append("")
        return "\n".join(lines)
    else:
        return f"📅 {first_date}\n\n{job_queue[0]['formatted_summary']}"

def process_batch_jobs():
    global job_queue, timer_thread, timer_start_time, last_sender_id

    if not job_queue:
        return

    target_sender_id = last_sender_id
    summary_text = generate_batch_summary()
    current_jobs = [job["text"] for job in job_queue]

    if summary_text:
        if GOOGLE_SPREADSHEET_ID:
            try:
                save_to_google_sheets(GOOGLE_SPREADSHEET_ID, current_jobs, job_queue)
                print("✅ บันทึกข้อมูลลง Google Sheets สำเร็จ")
            except Exception as e:
                print(f"❌ บันทึก Google Sheets ล้มเหลว: {e}")

        settings = load_settings()
        groups = settings.get("line_groups", [])
        active_groups = [g for g in groups if g.get("group_id")]

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            
            if active_groups:
                for grp in active_groups:
                    try:
                        push_request = PushMessageRequest(
                            to=grp.get("group_id"),
                            messages=[TextMessage(text=summary_text)]
                        )
                        line_bot_api.push_message(push_request)
                    except Exception as e:
                        print(f"❌ ส่งข้อความไปยังกลุ่ม LINE ({grp.get('name')}) ล้มเหลว: {e}")
            else:
                if target_sender_id:
                    try:
                        push_request = PushMessageRequest(
                            to=target_sender_id,
                            messages=[TextMessage(text=summary_text)]
                        )
                        line_bot_api.push_message(push_request)
                    except Exception as e:
                        print(f"❌ ส่งข้อความกลับหาผู้ส่งล้มเหลว: {e}")

        for job in latest_jobs:
            for q_job in job_queue:
                if job.get("order") == q_job.get("order") and job.get("id") == q_job.get("id"):
                    job["status"] = "ส่งแล้ว"

    job_queue.clear()
    timer_thread = None
    timer_start_time = None
    last_sender_id = None

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    received_text = event.message.text.strip()
    source_type = event.source.type
    user_id = event.source.user_id if hasattr(event.source, 'user_id') else None

    if source_type == 'group':
        group_id = event.source.group_id
        
        if received_text.lower() == "id":
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[TextMessage(text=f"{group_id}")]
                    )
                )
            return

    required_keywords = [
        "（接机รับ）",
        "【日期วันที่】",
        "【เวลาเวลา】",
        "【航班flight】",
        "【人数จำนวนคน】",
        "【行李กระเป๋า】",
        "【接รับ】",
        "【ส่งส่ง】",
        "【车型ขนาดรถ】",
        "【姓名ชื่อ】",
        "【电话เบอร์โทร】",
        "【客户订单号】",
        "【接驳编码code】",
        "【备注หมายเหต】"
    ]
    
    is_valid_form = all(keyword in received_text for keyword in required_keywords)

    if not is_valid_form:
        return  

    settings = load_settings()
    connected_groups = settings.get("line_groups", []) 

    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        
        if user_id:
            try:
                line_bot_api.push_message(
                    PushMessageRequest(
                        to=user_id,
                        messages=[TextMessage(text=f"📋 **ใบสรุปงาน (ส่งถึงคุณ):**\n\n{received_text}")]
                    )
                )
            except Exception as e:
                print(f"Push summary to personal chat error: {e}")

        if source_type == 'user':
            try:
                line_bot_api.reply_message(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[TextMessage(text=f"✅ ประมวลผลและส่งใบสรุปงานเข้ากลุ่มเรียบร้อยแล้ว")]
                    )
                )
            except Exception as e:
                print(f"Reply error: {e}")

        for group in connected_groups:
            g_id = group.get("group_id") 
            if g_id:
                try:
                    line_bot_api.push_message(
                        PushMessageRequest(
                            to=g_id,
                            messages=[TextMessage(text=f"{received_text}")]
                        )
                    )
                except Exception as e:
                    print(f"Push to group {g_id} error: {e}")

    add_job_to_queue(received_text, sender_id=user_id)
    
@app.route("/")
def index():
    return render_template("ui.html")

@app.route("/api/status", methods=["GET"])
def api_status():
    settings = load_settings()
    wait_seconds = int(settings.get("wait_seconds", 120))
    
    time_left = 0
    if timer_start_time:
        elapsed = time.time() - timer_start_time
        time_left = max(0, int(wait_seconds - elapsed))

    return jsonify({
        "line_status": bool(LINE_CHANNEL_ACCESS_TOKEN and LINE_CHANNEL_SECRET),
        "sheets_status": bool(GOOGLE_SPREADSHEET_ID and os.path.exists("credentials.json")),
        "queue_count": len(job_queue),
        "time_left_seconds": time_left,
        "max_wait_seconds": wait_seconds,
        "latest_jobs": latest_jobs,
        "custom_keywords": settings.get("custom_keywords", []),
        "line_groups": settings.get("line_groups", []),
        "settings": settings
    })

@app.route("/api/system_health", methods=["GET"])
def api_system_health():
    line_connected = False
    sheets_connected = False

    if LINE_CHANNEL_ACCESS_TOKEN and LINE_CHANNEL_SECRET:
        try:
            with ApiClient(configuration) as api_client:
                line_connected = True
        except Exception as e:
            print(f"LINE Bot check error: {e}")
            line_connected = False

    if GOOGLE_SPREADSHEET_ID:
        try:
            client = get_sheet_client()
            if client:
                client.open_by_key(GOOGLE_SPREADSHEET_ID)
                sheets_connected = True
        except Exception as e:
            print(f"Google Sheets check error: {e}")
            sheets_connected = False

    return jsonify({
        "line_bot": line_connected,
        "google_sheets": sheets_connected
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
        sheet_sum = spreadsheet.worksheet("SUMMARY")
        rows = sheet_sum.get_all_records()
        return jsonify({"success": True, "data": rows})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/add_job", methods=["POST"])
def api_add_job():
    data = request.get_json() or {}
    text = data.get("text", "").strip()
    if text:
        success = add_job_to_queue(text)
        return jsonify({"success": success, "message": "เพิ่มใบงานสำเร็จ" if success else "พบใบงานซ้ำ"})
    return jsonify({"success": False, "message": "ข้อความว่างเปล่า"}), 400

@app.route("/api/save_settings", methods=["POST"])
def api_save_settings():
    new_settings = request.get_json() or {}
    save_settings(new_settings)
    return jsonify({"success": True, "message": "บันทึกการตั้งค่าเรียบร้อยแล้ว"})

@app.route("/api/trigger_send", methods=["POST"])
def api_trigger_send():
    global timer_thread
    if timer_thread:
        timer_thread.cancel()
    process_batch_jobs()
    return jsonify({"success": True, "message": "ส่งสรุปใบงานเรียบร้อยแล้ว"})

@app.route('/api/delete_job', methods=['POST'])
@app.route("/api/delete_job/<job_id>", methods=["DELETE", "POST"])
def api_delete_job(job_id=None):
    try:
        if not job_id:
            data = request.get_json() or {}
            job_id = data.get('id')
        
        if not job_id:
            return jsonify({"success": False, "message": "ไม่พบรหัสใบงานที่ต้องการลบ"}), 400
        
        if not GOOGLE_SPREADSHEET_ID:
            return jsonify({"success": False, "message": "ยังไม่ได้ตั้งค่า GOOGLE_SPREADSHEET_ID"}), 400
        
        success = delete_row_from_google_sheets(GOOGLE_SPREADSHEET_ID, job_id)
        
        if success:
            return jsonify({"success": True, "message": f"ลบใบงาน {job_id} สำเร็จ"})
        else:
            return jsonify({"success": False, "message": "ไม่พบข้อมูลใน Google Sheets หรือเกิดข้อผิดพลาด"}), 404
            
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route("/api/clear_queue", methods=["POST"])
def api_clear_queue():
    global job_queue, timer_thread, timer_start_time
    if timer_thread:
        timer_thread.cancel()
    num_cleared = len(job_queue)
    job_queue.clear()
    timer_thread = None
    timer_start_time = None
    return jsonify({"success": True, "num_cleared": num_cleared, "message": "ล้าง Queue เรียบร้อยแล้ว"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
