import json
from google import genai
from google.genai import types

def summarize_jobs_with_ai(job_list, api_key):
    """
    ฟังก์ชันส่งข้อความใบงานทั้งหมดไปให้ Gemini API สรุปผล
    """
    if not job_list or not api_key:
        return "", []

    # สร้าง Client ของ Gemini
    client = genai.Client(api_key=api_key)

    prompt = f"""
คุณคือระบบผู้ช่วยสรุปใบงานรถรับส่ง ให้ทำตามกฎดังนี้อย่างเคร่งครัด:

กฎการแปลงข้อมูล:
1. กฎขนาดรถและราคา:
   - 5 SEAT = 5S = 380 (เช่น #5S หรือ #5D)
   - 7 SEAT = 7S = 480
   - Cam/7S = 7S = 480
2. กฎจุดรับ:
   - BKK = แอร์สุ
   - DMK = แอร์ดอน
   - อื่นๆ ให้ใช้ชื่อสั้นๆ
3. กฎจุดส่ง:
   - ย่อที่อยู่เหลือเพียง ชื่อเขต หรือ ชื่อพื้นที่ หรือ ชื่อถนนหลัก เท่านั้น
   - เช่น "1094/22 Vibhavadi Rangsit Rd, Chom Phon, Chatuchak, Bangkok, 10900" -> ให้แสดง "จตุจักร"
   - ห้ามใส่ที่อยู่เต็ม ห้ามเดาข้อมูล
4. รูปร่างแบบใบงานย่อ 1 ใบงาน:
   [รหัสใบงาน](🥶)[เวลา]/[ราคา]#[ขนาดรถ]
   [จุดรับ]-[จุดส่ง] ✈️[เที่ยวบิน]
   [เลข Order]

ตัวอย่างผลลัพธ์:
F05(🥶)01:33/380#5S
แอร์สุ-จตุจักร ✈️MU893
2370598381543194

หากมีหลายใบงาน ให้จัดเรียงลำดับตาม [เวลา] จากน้อยไปมาก และใส่บรรทัดแรกเป็น วันที่ (เช่น 📅 29/8/2026)

รายการใบงานต้นฉบับที่ต้องสรุป:
{"---".join(job_list)}

ให้ตอบกลับในรูปแบบ JSON ดังนี้:
{{
  "summary_text": "ข้อความสรุปทั้งหมดที่จะนำไปส่ง LINE Group",
  "parsed_items": [
    {{
      "job_code": "F05",
      "time": "01:33",
      "car": "5S",
      "price": "380",
      "pickup": "แอร์สุ",
      "dropoff": "จตุจักร",
      "flight": "MU893",
      "order_no": "2370598381543194",
      "short_text": "F05(🥶)01:33/380#5S\\nแอร์สุ-จตุจักร ✈️MU893\\n2370598381543194"
    }}
  ]
}}
"""

    try:
        # เรียกใช้ Gemini Model ( gemini-2.5-flash ) พร้อมบังคับให้ส่งคืนข้อมูลเป็น JSON
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.0
            )
        )
        
        result_json = json.loads(response.text)
        return result_json.get("summary_text", ""), result_json.get("parsed_items", [])
    except Exception as e:
        print(f"Error calling Gemini API: {e}")
        return "", []