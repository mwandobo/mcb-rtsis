// test-db2-connection.ts
import * as ibmdb from 'ibm_db';
import * as dotenv from 'dotenv';

dotenv.config();

const connStr =
  `DATABASE=${process.env.DB2_DATABASE};` +
  `HOSTNAME=${process.env.DB2_HOST};` +
  `PORT=${process.env.DB2_PORT ?? 50000};` +
  `PROTOCOL=TCPIP;` +
  `UID=${process.env.DB2_USER};` +
  `PWD=${process.env.DB2_PASSWORD};`;

console.log('🔌 Connecting to DB2...');
console.log(`   Host: ${process.env.DB2_HOST}:${process.env.DB2_PORT ?? 50000}`);
console.log(`   Database: ${process.env.DB2_DATABASE}`);
console.log(`   User: ${process.env.DB2_USER}\n`);

ibmdb.open(connStr, (err, conn) => {
  if (err) {
    console.error('❌ Connection failed:', err.message);
    process.exit(1);
  }

  console.log('✅ Connected successfully!\n');

  conn.query('SELECT CURRENT_TIMESTAMP AS NOW, CURRENT_USER AS WHO FROM SYSIBM.SYSDUMMY1', (queryErr, rows) => {
    if (queryErr) {
      console.error('❌ Test query failed:', queryErr.message);
    } else {
      console.log('📋 Server info:');
      console.table(rows);
    }

    conn.close(() => {
      console.log('\n🔒 Connection closed.');
      process.exit(0);
    });
  });
});