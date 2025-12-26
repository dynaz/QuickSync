import pyodbc

# ชื่อ DSN ของคุณ
DSN_NAME = "CData QB MAHA EN"

print("--- เริ่มต้นการทดสอบ ---")
try:
    # เชื่อมต่อ
    conn = pyodbc.connect(f'DSN={DSN_NAME};', autocommit=True)
    cursor = conn.cursor()
    
    # ใช้ SELECT * (เอาทุกอย่าง) และ TOP 1 (แค่แถวเดียว) เพื่อดูโครงสร้าง
    print(">> กำลังดึงข้อมูลจาก CompanyInfo (แบบ Select All)...")
    cursor.execute("SELECT TOP 1 * FROM CompanyInfo")
    
    # ดึงรายชื่อคอลัมน์ออกมาดู
    columns = [column[0] for column in cursor.description]
    print(f"✅ สำเร็จ! พบข้อมูล {len(columns)} คอลัมน์ ได้แก่:")
    print(columns) # โชว์รายชื่อคอลัมน์ทั้งหมดที่ CData เห็น
    
    # ดึงข้อมูลแถวแรกมาโชว์
    row = cursor.fetchone()
    if row:
        print("\nตัวอย่างข้อมูลบริษัท:", row)
        
    conn.close()

except Exception as e:
    print("\n❌ ยังมี Error อยู่:")
    print(e)

input("\nกด Enter เพื่อปิด...")