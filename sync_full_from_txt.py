import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
import datetime
import os

# ================= CONFIGURATION =================
# 1. QuickBooks Source (ODBC)
QB_DSN_NAME = "CData QB MAHA EN"
TABLE_LIST_FILE = "QuickBooks_Tables_List.txt"  # ชื่อไฟล์ที่คุณมี

# 2. PostgreSQL Destination
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "MAHA_EN"
PG_USER = "postgres"
PG_PASS = "9999"  # รหัสผ่านของคุณ

# =================================================

def get_postgres_engine():
    """สร้าง Connection Engine สำหรับ PostgreSQL"""
    encoded_pass = urllib.parse.quote_plus(PG_PASS)
    conn_str = f"postgresql+psycopg2://{PG_USER}:{encoded_pass}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(conn_str)

def get_qb_connection():
    """สร้าง Connection ไปหา QuickBooks ผ่าน CData ODBC"""
    return pyodbc.connect(f'DSN={QB_DSN_NAME};', autocommit=True)

def main():
    print(f"[{datetime.datetime.now()}] 🚀 Starting Full Sync Job")
    
    # 1. อ่านรายชื่อตารางจากไฟล์ .txt
    if not os.path.exists(TABLE_LIST_FILE):
        print(f"❌ ไม่พบไฟล์ {TABLE_LIST_FILE} กรุณาตรวจสอบว่าไฟล์วางอยู่ข้างๆ script หรือยัง")
        return

    # อ่านไฟล์และกรองเอาบรรทัด Header ออก (List of Tables..., =====)
    tables = []
    with open(TABLE_LIST_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            clean_line = line.strip()
            # เก็บเฉพาะบรรทัดที่มีตัวหนังสือ และไม่ใช่ Header
            if clean_line and not clean_line.startswith("List of") and not clean_line.startswith("=="):
                tables.append(clean_line)

    print(f"📋 พบรายชื่อตารางทั้งหมด: {len(tables)} ตาราง")
    print("-" * 30)
    
    pg_engine = get_postgres_engine()
    
    try:
        qb_conn = get_qb_connection()
        print("✅ Connected to QuickBooks via ODBC")
    except Exception as e:
        print(f"❌ Failed to connect to QuickBooks ODBC: {e}")
        return

    # 2. เริ่มวนลูปดึงทีละตาราง
    success_count = 0
    error_count = 0

    for i, table in enumerate(tables, 1):
        try:
            print(f"[{i}/{len(tables)}] ⏳ Processing: {table} ...", end='\r')
            
            # ใส่ Double Quote ครอบชื่อตาราง เพื่อความชัวร์
            # EXTRACT
            query = f'SELECT * FROM "{table}"'
            
            # 1. ใช้ Cursor รันคำสั่ง SQL แทน read_sql
            cursor = qb_conn.cursor()
            cursor.execute(query)
            
            # 2. ดึงชื่อคอลัมน์จาก cursor
            columns = [column[0] for column in cursor.description]
            
            # 3. ดึงข้อมูลทั้งหมดออกมา (fetchall)
            data = cursor.fetchall()
            
            # 4. สร้าง DataFrame จากข้อมูลดิบ (ต้องแปลงเป็น tuple เพื่อความชัวร์)
            # วิธีนี้ Pandas จะไม่บ่น เพราะเราสร้าง DataFrame ขึ้นมาเองตรงๆ
            df = pd.DataFrame([tuple(row) for row in data], columns=columns)
            
            # แปลงชื่อ Column เป็นตัวพิมพ์เล็ก
            df.columns = [c.lower() for c in df.columns]
            
            # ยัดลง Postgres
            if not df.empty:
                df.to_sql(table.lower(), pg_engine, if_exists='replace', index=False, chunksize=1000)
                print(f"[{i}/{len(tables)}] ✅ Success: {table} ({len(df)} rows)      ")
            else:
                print(f"[{i}/{len(tables)}] ⚠️  Skipped: {table} (No Data)           ")
            
            success_count += 1

        except Exception as e:
            # error ที่เจอบ่อยคือ ตารางที่ต้องใส่ Filter วันที่ หรือตาราง System
            print(f"\n[{i}/{len(tables)}] ❌ Failed: {table}")
            print(f"   Error: {e}")
            error_count += 1

    qb_conn.close()
    print("\n" + "="*50)
    print(f"🏁 Job Completed at {datetime.datetime.now()}")
    print(f"✅ Success: {success_count}")
    print(f"❌ Failed:  {error_count}")
    print("="*50)

if __name__ == "__main__":
    main()