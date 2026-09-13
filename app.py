import os
import re
import json
import time
import threading
import datetime
from flask import Flask, request, abort, render_template, jsonify, redirect, url_for
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

DISTRICT_MAP = {
    "CHATUCHAK": "จตุจักร",
    "PATHUM WAN": "ปทุมวัน",
    "SUKHUMVIT": "สุขุมวิท",
    "BANG RAK": "บางรัก",
    "PETCHABURI": "เพชรบุรีตัดใหม่",
    "RATCHATHEWI": "ราชเทวี",
    "BANG KAPI": "บางกะปิ",
    "DON MUEANG": "ดอนเมือง",
    "LAT KRABANG": "ลาดกระบัง"
}

def load_settings():
    if os.path.exists("settings.json"):
        with open("settings.json", "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                if "wait_seconds" not in data:
                    data["wait_seconds"] = 120
                return data
            except:
                pass
    return {"wait_seconds": 120, "line_groups": []}

def save_settings(data):
    current = {"wait_seconds": 120, "line_groups": []}
    if os.path.exists("settings.json"):
        with open("settings.json", "r", encoding="utf-8") as f:
            try:
                current = json.load(f)
            except:
                pass
                
    if "waiting_time" in data:
        try:
            val = float(data["waiting_time"])
            current["wait_seconds"] = int(val if val >= 30 else val * 60)
        except:
            pass
            
    if "line_groups" in data:
        current["line_groups"] = data["line_groups"]
    
    with open("settings.json", "w", encoding="utf-8") as f:
        json.dump(current, f, ensure_ascii=False, indent=2)

def map_dropoff_location(raw_dropoff):
    """แปลงจุดส่งเป็นเขต/พื้นที่"""
    if not raw_dropoff or raw_dropoff == "-":
        return "-"
    
    upper_dropoff = raw_dropoff.upper()
    for eng_key, thai_name in DISTRICT_MAP.items():
        if eng_key in upper_dropoff:
            if eng_key == "SUKHUMVIT":
                soi_match = re.search(r'SUKHUMVIT\s*(\d+)', upper_dropoff)
                if soi_match:
                    return f"สุขุมวิท {soi_match.group(1)}"
            return thai_name
            
    parts = raw_dropoff.split(',')
    return parts[0].strip()

def parse_job_text(raw_text, fallback_id="F01"):
    """แกะข้อมูลใบงาน รองรับจุดรับ จุดส่ง และแปลงค่าอัตโนมัติ"""
    if not raw_text:
        return {}

    lines = [line.strip() for line in raw_text.strip().split('\n') if line.strip()]
    
    id_match = re.search(r'(?:รหัสใบงาน|Job ID|ID)[:\s]*([A-Za-z0-9_-]+)', raw_text, re.IGNORECASE)
    if not id_match and lines:
        id_match = re.search(r'^([A-Za-z0-9_-]+)', lines[0])
    job_id = id_match.group(1).strip() if id_match else fallback_id

    date_match = re.search(r'(?:【(?:日期วันที่|日期|วันที่)】|วันที่|Date)[:\s]*([\d/\-]+)', raw_text, re.IGNORECASE)
    date_val = date_match.group(1).strip() if date_match else "-"

    time_match = re.search(r'(?:【(?:时间时间|时间|เวลา)】|เวลา|Time)[:\s]*([\d:]+)', raw_text, re.IGNORECASE)
    if not time_match:
        time_match = re.search(r'(\d{2}:\d{2})', raw_text)
    time_val = time_match.group(1).strip() if time_match else "-"

    flight_match = re.search(r'(?:【(?:航班flight|航班|flight)】|เที่ยวบิน|flight|Flight)[:\s]*([A-Za-z0-9]+)', raw_text, re.IGNORECASE)
    if not flight_match:
        flight_match = re.search(r'✈️?\s*([A-Za-z]{2}\d+|\d{3,4})', raw_text)
    flight_val = flight_match.group(1).strip() if flight_match else "-"

    # ตรวจสอบจุดรับ (Pickup)
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

    # ตรวจสอบจุดส่ง (Drop-off)
    dropoff_raw_match = re.search(r'(?:【(?:ส่งส่ง|送|ส่ง)】|จุดส่ง|Dropoff|Drop-off|To)[:\s]*(.+)', raw_text, re.IGNORECASE)
    if dropoff_raw_match:
        dropoff_raw = dropoff_raw_match.group(1).strip()
    else:
        found_dropoff = "-"
        upper_text = raw_text.upper()
        for eng_key in DISTRICT_MAP.keys():
            if eng_key in upper_text:
                found_dropoff = eng_key
                break
        dropoff_raw = found_dropoff

    dropoff_mapped = map_dropoff_location(dropoff_raw)

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

def is_duplicate(new_text):
    new_info = parse_job_text(new_text)
    new_order = new_info.get("order")
    new_id = new_info.get("id")

    for item in job_queue:
        existing_text = item.get("text", "").strip()
        existing_order = item.get("order", "")
        existing_id = item.get("id", "")

        if existing_text == new_text.strip():
            return True
        if new_order != "-" and existing_order == new_order:
            return True
        if new_id != "-" and existing_id == new_id:
            return True

    return False

def add_job_to_queue(text, sender_id=None):
    global job_queue, timer_thread, timer_start_time, latest_jobs, last_sender_id

    if is_duplicate(text):
        print("⚠️ พบใบงานซ้ำ ปฏิเสธการสร้าง Summary ซ้ำ")
        return False

    now_str = datetime.datetime.now().strftime("%H:%M:%S")
    fallback_id = f"F{len(latest_jobs) + 1:02d}"
    
    parsed_info = parse_job_text(text, fallback_id=fallback_id)

    job_item = {
        "id": parsed_info["id"],
        "date": parsed_info["date"] if parsed_info["date"] != "-" else datetime.datetime.now().strftime("%d/%m/%Y"),
        "text": text,
        "time": parsed_info["time"] if parsed_info["time"] != "-" else now_str,
        "pickup": parsed_info["pickup_raw"],        
        "pickup_display": parsed_info["pickup"],    
        "dropoff": parsed_info["dropoff_raw"],      
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
        lines = [f"📅 {first_date}"]
        for job in job_queue:
            lines.append(job["formatted_summary"])
        return "\n".join(lines)
    else:
        return job_queue[0]["formatted_summary"]

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
            except Exception as e:
                print(f"❌ บันทึก Google Sheets ล้มเหลว: {e}")

        settings = load_settings()
        groups = settings.get("line_groups", [])
        active_groups = [g for g in groups if g.get("enabled") and g.get("id")]

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            
            if active_groups:
                for grp in active_groups:
                    try:
                        push_request = PushMessageRequest(
                            to=grp.get("id"),
                            messages=[TextMessage(text=summary_text)]
                        )
                        line_bot_api.push_message(push_request)
                    except Exception as e:
                        print(f"❌ ส่งข้อความล้มเหลว: {e}")
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

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature', '')
    body = request.get_data(as_text=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    received_text = event.message.text
    source_type = event.source.type
    sender_id = None

    if source_type == 'group':
        group_id = event.source.group_id
        sender_id = group_id
        settings = load_settings()
        existing_ids = [g.get("id") for g in settings.get("line_groups", [])]
        if group_id not in existing_ids:
            settings.setdefault("line_groups", []).append({
                "id": group_id,
                "name": f"Group-{group_id[-4:]}",
                "enabled": True
            })
            save_settings(settings)
    elif source_type == 'room':
        sender_id = event.source.room_id
    elif source_type == 'user':
        sender_id = event.source.user_id

    add_job_to_queue(received_text, sender_id=sender_id)

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
        "settings": settings
    })

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