import os
from dotenv import load_dotenv
from google_sheet import save_to_google_sheets

load_dotenv()

spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
print(f"กำลังทดสอบเชื่อมต่อ Google Sheet ID: {spreadsheet_id}")

raw_test_data = ["F05\n01:33\nMU893\n2370598381543194"]
summary_test_data = [{
    "job_code": "F05",
    "time": "01:33",
    "car": "5S",
    "price": "380",
    "pickup": "แอร์สุ",
    "dropoff": "จตุจักร",
    "flight": "MU893",
    "order_no": "2370598381543194",
    "short_text": "F05(🥶)01:33/380#5S\nแอร์สุ-จตุจักร ✈️MU893\n2370598381543194"
}]

result = save_to_google_sheets(spreadsheet_id, raw_test_data, summary_test_data)

if result:
    print("✅ บันทึกลง Google Sheets สำเร็จเรียบร้อย!")
else:
    print("❌ บันทึกไม่สำเร็จ โปรดตรวจสอบ Error Log ด้านบน")