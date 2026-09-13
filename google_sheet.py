def get_all_summary_jobs(spreadsheet_id):
    """ดึงข้อมูลทั้งหมดจากชีต SUMMARY เพื่อนำไปแสดงผลหน้าเว็บ"""
    client = get_sheet_client()
    if not client or not spreadsheet_id:
        return []
    
    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        sheet_sum = spreadsheet.worksheet("SUMMARY")
        # gspread จะดึงข้อมูลมาเป็น List ของ Dictionary โดยใช้หัวตารางเป็น Key
        records = sheet_sum.get_all_records()
        return records
    except Exception as e:
        print(f"❌ Error fetching summary jobs: {e}")
        return []
