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

# Import local modules (เฉพาะส่วน Google Sheet)
from google_sheet import save_to_google_sheets, get_sheet_client, delete_row_from_google_sheets

load_dotenv()

app = Flask(__name__)

LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
GOOGLE_SPREADSHEET_ID = os.getenv("GOOGLE_SPREADSHEET_ID", "")

handler = WebhookHandler(LINE_CHANNEL_SECRET)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)

# Global State
job_queue = []          
timer_thread = None     
timer_start_time = None 
latest_jobs = []        
last_sender_id = None   

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
    "CAM/7S": {"code": "7S", "price": "480"},
    "CAMRY/7S": {"code": "7S", "price": "480"}
}

def load_settings():
    default_settings = {
        "wait_seconds": 120,
        "line_groups": [],
        "custom_keywords": [],
        "main_roads_dict": DEFAULT_MAIN_ROADS_DICT,
        "major_areas_dict": DEFAULT_MAJOR_AREAS_DICT,
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
                if "main_roads_dict" not in data:
                    data["main_roads_dict"] = DEFAULT_MAIN_ROADS_DICT
                if "major_areas_dict" not in data:
                    data["major_areas_dict"] = DEFAULT_MAJOR_AREAS_DICT
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

    if "main_roads_dict" in data:
        current["main_roads_dict"] = data["main_roads_dict"]

    if "major_areas_dict" in data:
        current["major_areas_dict"] = data["major_areas_dict"]

    if "custom_locations" in data:
        current["custom_locations"] = data["custom_locations"]
    
    with open("settings.json", "w", encoding="utf-8") as f:
        json.dump(current, f, ensure_ascii=False, indent=2)

def parse_location_rule_based(raw_location):
    """แปลงจุดรับ-จุดส่งด้วยกฎ และดึงพจนานุกรมล่าสุดจาก Settings"""
    if not raw_location or raw_location == "-":
        return "-"
    
    cleaned = raw_location.strip()
    upper_loc = cleaned.upper()
    
    # ดึงค่าพจนานุกรมปัจจุบันจาก settings
    settings = load_settings()
    current_main_roads = settings.get("main_roads_dict", DEFAULT_MAIN_ROADS_DICT)
    current_major_areas = settings.get("major_areas_dict", DEFAULT_MAJOR_AREAS_DICT)
    custom_keywords = settings.get("custom_keywords", [])

    # 0. ตรวจสอบ Custom Keywords ที่ผู้ใช้ตั้งค่าเพิ่มเอง
    for item in custom_keywords:
        kw = item.get("keyword", "").strip()
        target = item.get("zone", "").strip()
        if kw and kw.lower() in cleaned.lower():
            return target if target else cleaned

    # 0.1 เพิ่มตัวดักจับกรณีเศษข้อความหลุด เช่น "ei Nuea" จาก Khlong Toei Nuea ให้ตีเป็นสุขุมวิท
    if "EI NUEA" in upper_loc or "KHLONG TOEI" in upper_loc:
        return "สุขุมวิท"

    # 1. ตรวจจับสนามบินดอนเมือง (รองรับ DMK, T1-T5, Don Mueang, ดอนเมือง และคำว่าแอร์ดอนทั้งหมด)
    if any(k in upper_loc for k in ["DMK", "DON MUEANG", "ดอนเมือง", "แอร์ดอน"]) or re.search(r'DMK\s*T[1-5]', upper_loc):
        return "แอร์ดอน"
    
    # 2. ตรวจจับสนามบินสุวรรณภูมิ
    elif any(k in upper_loc for k in ["BKK", "SUVARNABHUMI", "SVB", "แอร์สุ", "สุวรรณภูมิ"]):
        return "แอร์สุ"
        
    # 3. ตรวจจับ Sukhumvit ตามด้วยเลขซอย ให้กลายเป็น สุขุมวิท
    if re.search(r'\bSukhumvit\s*\d+\b', cleaned, flags=re.IGNORECASE):
        return "สุขุมวิท"

    matched_th_road = ""

    # 4. ตรวจสอบชื่อถนนหลักจาก MAIN_ROADS_DICT
    if isinstance(current_main_roads, dict):
        for eng_road, th_road in current_main_roads.items():
            eng_items = eng_road if isinstance(eng_road, list) else [eng_road]
            th_items = th_road if isinstance(th_road, list) else [th_road]
            
            matched_eng = any(e and str(e).lower() in cleaned.lower() for e in eng_items if isinstance(e, str))
            matched_th = any(t and str(t) in cleaned for t in th_items if isinstance(t, str))
            
            if matched_eng or matched_th:
                matched_th_road = str(th_items[0]).strip() if th_items else str(eng_items[0]).strip()
                break
    elif isinstance(current_main_roads, list):
        for item in current_main_roads:
            key = item.get("key", "")
            aliases = item.get("aliases", [])
            alias_list = aliases if isinstance(aliases, list) else [aliases]
            if (key and str(key).lower() in cleaned.lower()) or any(a and str(a).lower() in cleaned.lower() for a in alias_list if isinstance(a, str)):
                matched_th_road = str(key).strip()
                break

    # 5. หากไม่เจอ ลองเช็คใน MAJOR_AREAS_DICT
    if not matched_th_road:
        if isinstance(current_major_areas, dict):
            for area_eng, area_th in current_major_areas.items():
                eng_items = area_eng if isinstance(area_eng, list) else [area_eng]
                th_items = area_th if isinstance(area_th, list) else [area_th]
                
                matched_eng = any(e and str(e).lower() in cleaned.lower() for e in eng_items if isinstance(e, str))
                matched_th = any(t and str(t) in cleaned for t in th_items if isinstance(t, str))
                
                if matched_eng or matched_th:
                    matched_th_road = str(th_items[0]).strip() if th_items else str(eng_items[0]).strip()
                    break
        elif isinstance(current_major_areas, list):
            for item in current_major_areas:
                key = item.get("key", "")
                aliases = item.get("aliases", [])
                alias_list = aliases if isinstance(aliases, list) else [aliases]
                if (key and str(key).lower() in cleaned.lower()) or any(a and str(a).lower() in cleaned.lower() for a in alias_list if isinstance(a, str)):
                    matched_th_road = str(key).strip()
                    break

    if matched_th_road:
        return matched_th_road

    # หากไม่ตรง ให้ตัดคำว่า "ซอย [ตัวเลข]" ออก
    cleaned_no_soi = re.sub(r'(?:ซอย|soi)\s*\d+', '', cleaned, flags=re.IGNORECASE).strip()
    
    return cleaned_no_soi if cleaned_no_soi else cleaned

def parse_job_line(line_text):
    parts = line_text.split('-')
    if len(parts) >= 2:
        pickup_raw = parts[0].strip()
        dropoff_raw = parts[1].strip()
        
        pickup_clean = parse_location_rule_based(pickup_raw)
        dropoff_clean = parse_location_rule_based(dropoff_raw)
        
        return pickup_clean, dropoff_clean
    return line_text, "-"

def parse_job_text(raw_text, fallback_id="F01"):
    if not raw_text:
        return {}

    lines = [line.strip() for line in raw_text.strip().split('\n') if line.strip()]
    
    # ปรับให้ดึงรหัส F ตามจริง ถ้าไม่มีค่อยใช้ fallback_id ที่รันให้อัตโนมัติ
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

    pickup_raw = "-"
    dropoff_raw = "-"
    pickup_mapped = "-"
    dropoff_mapped = "-"
    
    # 1. ดึงจากแท็ก 【接รับ】 และ 【ส่งส่ง】 พร้อมตัดวงเล็บหรือคอมมาส่วนเกินออกให้สะอาด
    pickup_raw_match = re.search(r'(?:【(?:接รับ|接|รับ)】|จุดรับ|Pickup|From)[:\s]*(.+)', raw_text, re.IGNORECASE)
    if pickup_raw_match:
        pickup_raw = pickup_raw_match.group(1).strip()
        pickup_raw = re.split(r'[\(,\)]', pickup_raw)[0].strip()

    dropoff_raw_match = re.search(r'(?:【(?:送ส่ง|ส่ง|ส่ง)】|จุดส่ง|Dropoff|Drop-off|To)[:\s]*(.+)', raw_text, re.IGNORECASE)
    if dropoff_raw_match:
        dropoff_raw = dropoff_raw_match.group(1).strip()
        dropoff_raw = re.split(r'[\(,\)]', dropoff_raw)[0].strip()

    # 2. หากในแท็กไม่มีข้อมูล ค่อยไปหาจากบรรทัดที่มีเครื่องหมาย - ทั่วไป
    if pickup_raw == "-" or dropoff_raw == "-":
        route_line = ""
        for line in lines:
            if "-" in line and not any(k in line for k in ["【", "รหัส", "Order"]):
                route_line = line
                break
        if route_line:
            parts = route_line.split("-", 1)
            if pickup_raw == "-":
                pickup_raw = parts[0].strip()
            if dropoff_raw == "-":
                dropoff_raw = parts[1].strip()
                dropoff_raw = re.split(r'[\(,\)]', dropoff_raw)[0].strip()

    # แปลงผ่าน Rule-based
    pickup_mapped = parse_location_rule_based(pickup_raw)
    dropoff_mapped = parse_location_rule_based(dropoff_raw)

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

def add_job_to_queue(text, sender_id=None):
    global job_queue, timer_thread, timer_start_time, latest_jobs, last_sender_id

    now_str = datetime.datetime.now().strftime("%H:%M:%S")
    
    next_seq = len(job_queue) + len(latest_jobs) + 1
    fallback_id = f"F{next_seq:02d}"
    
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
    latest_jobs = latest_jobs[:50]

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
        raw_date = job.get("date", datetime.datetime.now().strftime("%d/%m/%Y"))
        try:
            parsed_d = datetime.datetime.strptime(raw_date.replace('-', '/'), "%d/%m/%Y")
            job_date = parsed_d.strftime("%d/%m/%Y")
        except:
            job_date = raw_date

        if job_date not in date_groups:
            date_groups[job_date] = []
        date_groups[job_date].append(job)

    blocks = []
    sorted_dates = sorted(date_groups.keys(), key=lambda x: datetime.datetime.strptime(x, "%d/%m/%Y"))

    for d in sorted_dates:
        blocks.append(f"📅 {d}")
        blocks.append("")
        
        sorted_jobs = sorted(date_groups[d], key=lambda x: x.get("id", ""))
        
        for i, job in enumerate(sorted_jobs):
            blocks.append(job["formatted_summary"])
            if i < len(sorted_jobs) - 1:
                blocks.append("")
                
        if d != sorted_dates[-1]:
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
        "main_roads_dict": settings.get("main_roads_dict", DEFAULT_MAIN_ROADS_DICT),
        "major_areas_dict": settings.get("major_areas_dict", DEFAULT_MAJOR_AREAS_DICT),
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
