import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import datetime
import os

# ================= CONFIGURATION =================
# 1. Source & List File
QB_DSN_NAME = "CData QB MAHA EN"
TABLE_LIST_FILE = "QuickBooks_Tables_List.txt"  # ไฟล์รายชื่อ 84 ตาราง

# 2. PostgreSQL Destination
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "MAHA_2018"
PG_USER = "postgres"
PG_PASS = "9999"  # <--- ใส่รหัสผ่าน Postgres

# =================================================

def get_postgres_engine():
    conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(conn_str)

def main():
    print(f"[{datetime.datetime.now()}] 🚀 Starting Bulk Sync Job")
    
    # 1. อ่านรายชื่อตารางจากไฟล์ .txt
    if not os.path.exists(TABLE_LIST_FILE):
        print(f"❌ ไม่พบไฟล์ {TABLE_LIST_FILE} กรุณาสร้างไฟล์รายชื่อก่อน")
        return

    with open(TABLE_LIST_FILE, 'r', encoding='utf-8') as f:
        # อ่านทีละบรรทัด, ตัดช่องว่าง, ข้ามบรรทัดว่าง และข้ามบรรทัด Header ที่ไม่ใช่ชื่อตาราง
        tables = [line.strip() for line in f if line.strip() and not line.startswith("List of") and not line.startswith("===")]

    print(f"📋 พบรายชื่อตารางที่จะดึงทั้งหมด: {len(tables)} ตาราง")
    
    pg_engine = get_postgres_engine()
    
    # เชื่อมต่อ ODBC ครั้งเดียว
    try:
        qb_conn = pyodbc.connect(f'DSN={QB_DSN_NAME};', autocommit=True)
        print("✅ Connected to QuickBooks ODBC")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # 2. วนลูปดึงข้อมูล
    success_count = 0
    error_count = 0

    for i, table in enumerate(tables, 1):
        print(f"[{i}/{len(tables)}] Processing: {table} ...", end='\r')
        
        try:
            # EXTRACT
            query = f"SELECT * FROM \"{table}\"" # ใส่ Double Quote กันชื่อตารางมีเว้นวรรค
            
            # ใช้ pandas อ่านข้อมูล (chunksize ช่วยลด Ram กรณีข้อมูลเยอะ)
            df = pd.read_sql(query, qb_conn)
            
            # TRANSFORM: แปลงชื่อคอลัมน์เป็นตัวเล็กทั้งหมด (Postgres Best Practice)
            df.columns = [c.lower() for c in df.columns]
            
            # LOAD: ลง Postgres
            if not df.empty:
                # if_exists='replace' = ลบตารางเก่าสร้างใหม่ (Full Load)
                df.to_sql(table.lower(), pg_engine, if_exists='replace', index=False)
                print(f"✅ [{i}/{len(tables)}] Success: {table} ({len(df)} rows)      ")
            else:
                print(f"⚠️ [{i}/{len(tables)}] Skipped: {table} (No Data)            ")
            
            success_count += 1

        except Exception as e:
            print(f"\n❌ [{i}/{len(tables)}] Failed: {table}")
            print(f"   Error: {e}")
            error_count += 1

    qb_conn.close()
    print("\n" + "="*50)
    print(f"🏁 Job Completed at {datetime.datetime.now()}")
    print(f"✅ Success: {success_count}")
    print(f"❌ Errors:  {error_count}")
    print("="*50)

if __name__ == "__main__":
    main()