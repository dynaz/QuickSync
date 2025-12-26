import odbc from 'odbc';
import { companyFiles } from './config';
import { processOrdersToPostgres } from './etl-logic'; // Logic เดิมที่คุณมี

async function runBatchJob() {
  console.log("🚀 Starting Multi-Company Batch Job...");

  for (const company of companyFiles) {
    console.log(`Processing: ${company.name} (${company.id})`);
    
    let connection;
    try {
      // เทคนิค: การ Connect แบบไม่ต้องสร้าง DSN เยอะ (Connection String Injection)
      // บาง Driver อนุญาตให้ระบุ Path ไฟล์ตรงๆ ได้เลย (ต้องเช็ค Manual ของ Driver ที่ใช้)
      const connString = `DSN=QuickBooks Data;DFQ=${company.filePath};OpenMode=F;`; 
      
      // หรือถ้าใช้แบบ DSN แยกก็: `DSN=${company.dsnName}`
      connection = await odbc.connect(connString);

      // 1. EXTRACT
      const orders = await connection.query(`SELECT * FROM SalesOrder WHERE TimeModified >= {fn NOW()} - 1`); // ดึงเฉพาะของวันนี้
      
      if (orders.length > 0) {
          // 2. TRANSFORM & 3. LOAD (ส่ง companyId ไปด้วยเพื่อแยกข้อมูลใน Postgres)
          await processOrdersToPostgres(orders, company.id);
          console.log(`✅ Success: ${orders.length} orders processed.`);
      } else {
          console.log(`ℹ️ No new updates.`);
      }

    } catch (error) {
      console.error(`❌ Error processing ${company.name}:`, error);
      // สำคัญ: อย่าให้ Error ไฟล์เดียวทำระบบล่มทั้ง Loop ให้ใช้ try/catch ดักไว้
    } finally {
      if (connection) await connection.close(); // ปิด Connection เสมอก่อนไปไฟล์ถัดไป
    }
  }

  console.log("🏁 Batch Job Completed.");
}

runBatchJob();