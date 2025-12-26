import pyodbc

# ชื่อ DSN ของคุณ
DSN_NAME = "MAHA"
OUTPUT_FILE = "QuickBooks_Tables_List.txt"

print(f"--- 🚀 เริ่มต้นดึงรายชื่อ Table จาก {DSN_NAME} ---")

try:
    # 1. เชื่อมต่อ ODBC
    conn = pyodbc.connect(f'DSN={DSN_NAME};', autocommit=True)
    cursor = conn.cursor()
    
    # 2. คำสั่งพิเศษของ ODBC เพื่อขอดูรายชื่อ Table ทั้งหมด
    # cursor.tables() เป็นคำสั่งมาตรฐานที่ใช้ดึง Metadata
    print(">> กำลังดึง Metadata รายชื่อตาราง...")
    tables = cursor.tables(tableType='TABLE').fetchall()
    
    # 3. เตรียมข้อมูลที่จะบันทึก
    table_names = []
    for table in tables:
        # table.table_name คือ property ที่เก็บชื่อตาราง
        if table.table_name:
            table_names.append(table.table_name)
            
    # เรียงตามตัวอักษรเพื่อให้อ่านง่าย
    table_names.sort()

    # 4. บันทึกลงไฟล์ .txt
    print(f">> พบทั้งหมด {len(table_names)} ตาราง กำลังบันทึกลงไฟล์...")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(f"List of Tables in {DSN_NAME}\n")
        f.write("="*40 + "\n")
        for name in table_names:
            f.write(f"{name}\n")

    print(f"✅ เรียบร้อย! บันทึกไฟล์ชื่อ: {OUTPUT_FILE}")
    print(f"   (ไฟล์จะอยู่ที่เดียวกับ Script นี้)")
    
    conn.close()

except Exception as e:
    print("\n❌ เกิดข้อผิดพลาด:")
    print(e)

input("\nกด Enter เพื่อปิด...")