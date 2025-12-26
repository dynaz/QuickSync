import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
import datetime

# ================= CONFIGURATION =================
# 1. QuickBooks Source (ODBC)
QB_DSN_NAME = "Internal"

# 2. PostgreSQL Destination
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "Internal"
PG_USER = "postgres"
PG_PASS = "@Dmin2019" 

# 3. รายชื่อตารางอ่านจากไฟล์ .txt
# =================================================

def get_postgres_engine():
    """สร้าง Connection Engine สำหรับ PostgreSQL"""
    encoded_pass = urllib.parse.quote_plus(PG_PASS)
    conn_str = f"postgresql+psycopg2://{PG_USER}:{encoded_pass}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(conn_str)

def get_qb_connection():
    """สร้าง Connection ไปหา QuickBooks ผ่าน CData ODBC"""
    return pyodbc.connect(f'DSN={QB_DSN_NAME};', autocommit=True)

def load_tables_from_file(filepath):
    """อ่านรายชื่อตารางจากไฟล์ .txt"""
    try:
        with open(filepath, 'r') as f:
            tables = [line.strip() for line in f.readlines() if line.strip()]
        print(f"✅ Loaded {len(tables)} tables from {filepath}")
        return tables
    except Exception as e:
        print(f"❌ Failed to load tables from file: {e}")
        return []

def main():
    print(f"[{datetime.datetime.now()}] 🚀 Starting Job: QuickBooks -> PostgreSQL")
    
    # โหลดรายชื่อตารางจากไฟล์ .txt
    tables_to_sync = load_tables_from_file("QuickBooks_Tables_List.txt")
    
    pg_engine = get_postgres_engine()
    
    try:
        qb_conn = get_qb_connection()
        print("✅ Connected to QuickBooks via ODBC")
    except Exception as e:
        print(f"❌ Failed to connect to QuickBooks ODBC: {e}")
        return

    for table in tables_to_sync:
        try:
            print(f"   ⏳ Processing table: {table} ...", end='\r')
            
            # ใส่ Double Quote ครอบชื่อตาราง เพื่อความชัวร์
            query = f'SELECT * FROM "{table}"'
            
            # อ่านข้อมูลจาก QB
            df = pd.read_sql(query, qb_conn)
            
            # แปลงชื่อ Column เป็นตัวพิมพ์เล็ก
            df.columns = [c.lower() for c in df.columns]
            
            # ยัดลง Postgres - สร้างตารางเสมอ แม้ไม่มีข้อมูล
            if not df.empty:
                df.to_sql(table.lower(), pg_engine, if_exists='replace', index=False, chunksize=1000)
                print(f"   ✅ Processed table: {table} ({len(df)} rows)      ")
            else:
                # สร้างตารางแม้ไม่มีข้อมูล โดยใช้ WHERE 1=0 เพื่อดึง Schema
                # วิธีนี้จะได้โครงสร้างตารางโดยไม่ต้องดึงข้อมูลจริง
                schema_query = f'SELECT * FROM "{table}" WHERE 1=0'
                schema_df = pd.read_sql(schema_query, qb_conn)
                schema_df.columns = [c.lower() for c in schema_df.columns]
                # สร้างตารางด้วย DataFrame ว่างที่มี Schema ถูกต้อง
                schema_df.to_sql(table.lower(), pg_engine, if_exists='replace', index=False)
                print(f"   ✅ Created table: {table} (0 rows, schema created)      ")

        except Exception as e:
            print(f"\n   ❌ Error processing {table}: {e}")

    qb_conn.close()
    print(f"[{datetime.datetime.now()}] 🏁 Job Completed Successfully.")

if __name__ == "__main__":
    main()