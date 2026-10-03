# ตั้งค่า Supabase (ข้อมูลไม่หายตอน Render หลับ)

## 1) สร้างตาราง
1. เข้า https://supabase.com → สร้าง Project (ฟรีได้)
2. เมนูซ้าย **SQL Editor** → New query → วางโค้ดในไฟล์ `supabase_setup.sql` → กด **Run**

## 2) เอาคีย์มาใส่ใน Render
1. Supabase → **Project Settings → API** (หรือ Data API / API Keys)
   - คัดลอก **Project URL**
   - คัดลอก **service_role key** (หรือ secret key) — ห้ามแชร์ให้ใคร
2. Render → เลือก service → **Environment** → เพิ่ม
   - `SUPABASE_URL` = Project URL
   - `SUPABASE_SERVICE_ROLE_KEY` = service_role key
3. Render → **Settings → Start Command** ตั้งเป็น
   `gunicorn app:app --workers 1 --threads 8 --timeout 120`
   (ต้องเป็น 1 worker เพราะคิวใบงานอยู่ในโปรแกรมเดียว)
4. อัปโหลดโค้ดใหม่ขึ้น GitHub → Render จะ Deploy เอง

## 3) เช็คว่าใช้ได้
- เปิดหน้าเว็บ → กล่อง **System Status** ต้องเห็น `ฐานข้อมูล (Supabase): Connected`
- เพิ่มถนนหลัก 1 อัน → กดบันทึก → Render: **Manual Deploy → Restart** → เปิดหน้าเว็บใหม่ ถนนที่เพิ่มต้องยังอยู่
- ครั้งแรกที่ต่อ Supabase ระบบจะใช้ค่าเริ่มต้น ถนน/กลุ่ม LINE ที่หายไปแล้วต้องเพิ่มใหม่ 1 ครั้ง หลังจากนั้นไม่หายอีก

## สิ่งที่ถูกเก็บใน Supabase (ตาราง `bot_store`)
| key | เก็บอะไร |
|---|---|
| `settings` | เวลารอ, กลุ่ม LINE, ถนนหลัก, ย่านสำคัญ, คำค้นเพิ่มเติม |
| `job_state` | คิวใบงานที่รอส่ง + ประวัติใบงานล่าสุด 100 ใบ |

## กันเซิร์ฟเวอร์หลับ
- โค้ดใหม่จะ ping ตัวเองทุก 10 นาทีอัตโนมัติบน Render (ปิดได้ด้วย env `KEEP_ALIVE=0`)
- แนะนำเพิ่ม: สมัคร https://cron-job.org หรือ UptimeRobot ให้เรียก `https://<ชื่อเว็บ>.onrender.com/health` ทุก 10 นาที (กันกรณีเครื่องหลับไปแล้ว)
- LINE Developers → Messaging API → เปิด **Webhook redelivery** (ถ้าเครื่องตื่นช้า LINE จะส่งข้อความซ้ำให้ ระบบกันใบงานซ้ำให้แล้ว)
