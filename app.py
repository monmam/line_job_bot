import os
import re
import json
import time
import threading
import datetime
from flask import Flask, request, abort, render_template, jsonify, redirect, url_for
from linebot.v3.messaging import TextMessage, ReplyMessageRequest
from dotenv import load_dotenv
from google import genai

# Import LINE SDK (v3)
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, PushMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

# Import local modules (เฉพาะส่วน Google Sheet)
from google_sheet import save_to_google_sheets, get_sheet_client, delete_row_from_google_sheets

load_dotenv()

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "")

# ตั้งค่า Gemini API Key
client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

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

def summarize_jobs_with_ai(jobs_text):
    """ฟังก์ชันสำหรับสรุปใบงานด้วย AI"""
    if not client:
        return jobs_text
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=f"กรุณาสรุปข้อมูลใบงานเหล่านี้ให้กระชับ:\n{jobs_text}"
        )
        return response.text.strip()
    except Exception as e:
        print(f"AI Summary Error: {e}")
        return jobs_text

def extract_pickup_dropoff_with_gemini(raw_text):
    """ให้ Gemini AI วิเคราะห์ข้อความใบงานเพื่อดึงจุดรับและจุดส่งที่สะอาดและถูกต้องแม่นยำที่สุด"""
    if not client:
        return "-", "-"
    try:
        prompt = f"""
คุณเป็นระบบ AI ผู้เชี่ยวชาญการจัดการข้อมูลการเดินทางในกรุงเทพฯ จงวิเคราะห์ข้อความใบงานด้านล่างนี้ แล้วแยก "จุดรับ" (Pickup) และ "จุดส่ง" (Dropoff) ให้ถูกต้อง

**กฎเหล็กในการแปลงชื่อสถานที่:**
1. หากเป็นสนามบิน ให้ใช้คำว่า "แอร์ดอน" (สำหรับ DMK) หรือ "แอร์สุ" (สำหรับ BKK) เท่านั้น
2. สำหรับสถานที่ทั่วไป ให้ดึงเฉพาะ: **ถนนหลัก**, **ซอยที่มีเลข** (เช่น สุขุมวิท 39, พหลโยธิน 3), หรือ **ย่านสำคัญ / แหล่งท่องเที่ยว / เขตพื้นที่** (เช่น ข้าวสาร, สยาม, สาทร, เพชรบุรี, พญาไท)
3. **ห้ามมีชื่อโรงแรมหรือชื่อตึกเต็มๆ หลุดมาเด็ดขาด** (ให้ตัดคำว่า Holiday Inn, Eastin Grand, The Standard, Anantara ทิ้งทั้งหมด)
4. ความยาวแต่ละจุดต้องสั้นกระชับไม่เกิน 2-4 คำเท่านั้น

ข้อความใบงาน:
{raw_text}

โปรดตอบกลับในรูปแบบ JSON เท่านั้น โดยมี Key เป็น "pickup" และ "dropoff" เช่น:
{{"pickup": "แอร์ดอน", "dropoff": "สยาม"}}
"""
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config={"response_mime_type": "application/json"}
        )
        data = json.loads(response.text.strip())
        return data.get("pickup", "-"), data.get("dropoff", "-")
    except Exception as e:
        print(f"Gemini Extract Error: {e}")
        return "-", "-"
        
def parse_job_line(line_text):
    """ฟังก์ชันแยกและจัดการจุดรับ-จุดส่งจากข้อความดิบ"""
    parts = line_text.split('-')
    if len(parts) >= 2:
        pickup_raw = parts[0].strip()
        dropoff_raw = parts[1].strip()
        
        pickup_clean = smart_parse_location_with_gemini(pickup_raw)
        dropoff_clean = smart_parse_location_with_gemini(dropoff_raw)
        
        return pickup_clean, dropoff_clean
    return line_text, "-"

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

def parse_job_text(raw_text, fallback_id="F01"):
    """แกะข้อมูลใบงานและดึงจุดรับ-จุดส่งด้วย Gemini AI อย่างแม่นยำ"""
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
    time_match = re.search(r'(?:【(?:เวลาเวลา|เวลา|시간시간|시간)】|เวลา|Time)[:\s]*([\d:]+)', raw_text, re.IGNORECASE)
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

    # 5. ใช้ AI สกัดจุดรับและจุดส่งแบบแม่นยำ
    pickup_mapped, dropoff_mapped = extract_pickup_dropoff_with_gemini(raw_text)

    # 6. ขนาดรถและราคา (Car & Price)
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

    # 7. หมายเลขคำสั่งซื้อ (Order)
    order_match = re.search(r'(?:【(?:客户订单号|订单号|Order)】|Order|Order Number|คำสั่งซื้อ|Order ID)[:\s]*([0-9A-Za-z_-]+)', raw_text, re.IGNORECASE)
    if not order_match:
        order_match = re.search(r'\b(\d{8,20})\b', raw_text)
    order_val = order_match.group(1).strip() if order_match else "-"

    formatted_summary = f"{job_id}(🥶){time_val}/{price_val}#{car_code}\n{pickup_mapped}-{dropoff_mapped} ✈️{flight_val}\n{order_val}"

    return {
        "id": job_id,
        "date": date_val,
        "time": time_val,
        "pickup_raw": pickup_mapped,
        "pickup": pickup_mapped,
        "dropoff_raw": dropoff_mapped,
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

    date_groups = {}
    for job in job_queue:
        job_date = job.get("date", datetime.datetime.now().strftime("%d/%m/%Y"))
        if job_date not in date_groups:
            date_groups[job_date] = []
        date_groups[job_date].append(job)

    blocks = []
    for d, jobs in date_groups.items():
        blocks.append(f"📅 {d}")
        blocks.append("")
        for i, job in enumerate(jobs):
            blocks.append(job["formatted_summary"])
            if i < len(jobs) - 1:
                blocks.append("")
        if d != list(date_groups.keys())[-1]:
            blocks.append("")

    return "\n".join(blocks)

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
            
            if target_sender_id:
                try:
                    push_request = PushMessageRequest(
                        to=target_sender_id,
                        messages=[TextMessage(text=summary_text)]
                    )
                    line_bot_api.push_message(push_request)
                except Exception as e:
                    print(f"❌ ส่งข้อความกลับหาผู้ส่ง (LINE OA) ล้มเหลว: {e}")

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

    is_valid_form = "【客户订单号】" in received_text

    if not is_valid_form:
        return  
        
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
