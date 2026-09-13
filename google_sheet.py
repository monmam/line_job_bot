import datetime
import gspread
from google.oauth2.service_account import Credentials

def get_sheet_client():
    """เชื่อมต่อ Google Sheets ด้วย Service Account credentials.json"""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    try:
        creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"Google Sheets Auth Error: {e}")
        return None

def normalize_pickup(text):
    """แปลงค่าจุดรับตามเงื่อนไข (BKK -> แอร์สุ, DMK / DMK T1 -> แอร์ดอน)"""
    if not text:
        return "-"
    t = text.strip().upper()
    if "BKK" in t:
        return "แอร์สุ"
    if "DMK" in t:
        return "แอร์ดอน"
    return text

def normalize_dropoff(text):
    """แปลงค่าจุดส่งจากภาษาอังกฤษเป็นภาษาไทยตามเขต/สถานที่สำคัญ"""
    if not text:
        return "-"
    
    mapping = {
        "chatuchak": "จตุจักร",
        "pathum wan": "ปทุมวัน",
        "sukhumvit": "สุขุมวิท",
        "bang rak": "บางรัก",
        "siam": "สยาม",
        "silom": "สีลม",
        "sathorn": "สาทร",
        "huai khwang": "ห้วยขวาง",
        "wattana": "วัฒนา",
        "ratchathewi": "ราชเทวี"
    }
    
    t_lower = text.strip().lower()
    for eng, th in mapping.items():
        if eng in t_lower:
            return th
            
    return text

def save_to_google_sheets(spreadsheet_id, raw_jobs, summary_items):
    """
    บันทึกข้อมูลลง Sheet RAW_JOBS และ SUMMARY พร้อมแปลงข้อมูลจุดรับ-จุดส่ง
    """
    client = get_sheet_client()
    if not client:
        return False

    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        # 1. บันทึกลง RAW_JOBS
        try:
            sheet_raw = spreadsheet.worksheet("RAW_JOBS")
        except:
            sheet_raw = spreadsheet.add_worksheet(title="RAW_JOBS", rows="1000", cols="10")
            sheet_raw.append_row(["วันที่รับ", "วันที่ใบงาน", "รหัสใบงาน", "เวลา", "ข้อความใบงานต้นฉบับ", "Order Number", "สถานะ"])

        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        raw_rows = []
        for raw in raw_jobs:
            raw_rows.append([now_str, "-", "-", "-", raw, "-", "PROCESSED"])
        sheet_raw.append_rows(raw_rows)

        # 2. บันทึกลง SUMMARY (เรียงลำดับคอลัมน์ A ถึง K ให้ตรงกับ Google Sheets)
        try:
            sheet_sum = spreadsheet.worksheet("SUMMARY")
        except:
            sheet_sum = spreadsheet.add_worksheet(title="SUMMARY", rows="1000", cols="12")
            sheet_sum.append_row(["วันที่", "รหัสใบงาน", "เวลา", "รถ", "ราคา", "จุดรับ", "จุดส่ง", "เที่ยวบิน", "Order Number", "ข้อความใบงานย่อ", "เวลาที่ส่ง"])

        sum_rows = []
        for item in summary_items:
            raw_pickup = item.get("pickup", "")
            raw_dropoff = item.get("dropoff", "")

            sum_rows.append([
                item.get("date", now_str.split()[0]),        # A: วันที่
                item.get("id", ""),                          # B: รหัสใบงาน
                item.get("time", ""),                        # C: เวลา
                item.get("car_code", ""),                    # D: รถ
                item.get("price", ""),                       # E: ราคา
                normalize_pickup(raw_pickup),                # F: จุดรับ (แปลงค่าแล้ว)
                normalize_dropoff(raw_dropoff),              # G: จุดส่ง (แปลงค่าแล้ว)
                item.get("flight", ""),                      # H: เที่ยวบิน
                item.get("order", ""),                       # I: Order Number
                item.get("formatted_summary", ""),           # J: ข้อความใบงานย่อ
                now_str                                      # K: เวลาที่ส่ง
            ])
        sheet_sum.append_rows(sum_rows)
        return True
    except Exception as e:
        print(f"Google Sheet Save Error: {e}")
        return False

def delete_row_from_google_sheets(spreadsheet_id, target_id):
    """ลบแถวข้อมูลใน Google Sheets (ทั้ง SUMMARY และ RAW_JOBS) รองรับทั้งการค้นหาด้วย ID, Order และข้อความ"""
    client = get_sheet_client()
    if not client or not spreadsheet_id:
        return False
    
    target_str = str(target_id).strip()
    success = False
    order_number_to_find = None
    row_text_to_find = None
    
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        # 1. ค้นหาและลบจากชีต SUMMARY พร้อมเก็บข้อมูลสำคัญไว้ใช้เทียบกับ RAW_JOBS
        try:
            sheet_sum = spreadsheet.worksheet("SUMMARY")
            records_sum = sheet_sum.get_all_records()
            row_to_delete_sum = None
            
            for idx, row in enumerate(records_sum):
                job_id_val = str(row.get("รหัสใบงาน", "")).strip()
                order_val = str(row.get("Order Number", "")).strip()
                
                if job_id_val == target_str or order_val == target_str or target_str in job_id_val:
                    row_to_delete_sum = idx + 2 # บวก 2 ชดเชย Header และ 0-index
                    if order_val and order_val != "-":
                        order_number_to_find = order_val
                    break
            
            if row_to_delete_sum:
                # ดึงข้อความหรือรายละเอียดของแถวนี้ก่อนลบ เพื่อเอาไปเทียบกับ RAW_JOBS
                row_values_sum = sheet_sum.row_values(row_to_delete_sum)
                sheet_sum.delete_rows(row_to_delete_sum)
                print(f"🗑️ ลบแถวที่ {row_to_delete_sum} ในชีต SUMMARY สำเร็จ")
                success = True
        except Exception as e:
            print(f"⚠️ Error deleting from SUMMARY: {e}")

        # 2. ลบจากชีต RAW_JOBS
        try:
            sheet_raw = spreadsheet.worksheet("RAW_JOBS")
            rows_raw = sheet_raw.get_all_values()
            
            # วนลูปจากล่างขึ้นบนเพื่อป้องกัน index เพี้ยน
            for idx in range(len(rows_raw) - 1, 0, -1):
                row_values = rows_raw[idx]
                row_str = " ".join(str(val) for val in row_values)
                
                match_found = False
                if order_number_to_find and order_number_to_find in row_str:
                    match_found = True
                elif target_str in row_str:
                    match_found = True
                    
                if match_found:
                    sheet_raw.delete_rows(idx + 1)
                    print(f"🗑️ ลบแถวที่ {idx + 1} ในชีต RAW_JOBS สำเร็จ")
                    success = True
                    # ไม่ break ทันที เผื่อมีหลายแถวที่ตรงกัน
        except Exception as e:
            print(f"⚠️ Error deleting from RAW_JOBS: {e}")

        return success
    except Exception as e:
        print(f"❌ Google Sheet Delete Error: {e}")
        return False