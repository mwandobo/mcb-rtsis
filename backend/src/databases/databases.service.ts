import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '../config/config.service';
import * as fs from 'fs';
import * as path from 'path';

@Injectable()
export class DatabasesService {
  private readonly logger = new Logger(DatabasesService.name);

  constructor(private readonly configService: ConfigService) {}

  private getDb2Connection() {
    const ibmdb = require('ibm_db');
    const dbConfig = this.configService.getDatabaseConfig();
    const connStr = `DATABASE=${dbConfig.database};HOSTNAME=${dbConfig.host};PORT=${dbConfig.port};PROTOCOL=TCPIP;UID=${dbConfig.username};PWD=${dbConfig.password};`;
    return { ibmdb, connStr };
  }

  async getDb2Tables(): Promise<any[]> {
    try {
      const { ibmdb, connStr } = this.getDb2Connection();
      
      this.logger.log(`🔌 DB2: Connecting to ${this.configService.get<string>('DB_HOST')}:${this.configService.get<string>('DB_PORT')}/${this.configService.get<string>('DB_NAME')} as ${this.configService.get<string>('DB_USERNAME')}...`);
      
      const connection = await ibmdb.open(connStr);
      
      this.logger.log(`✅ DB2: Connected successfully`);

      // Pipeline summary queries folder - go up one level from backend to project root
      const summariesDir = path.resolve(process.cwd(), '..', 'sqls', 'db2-summaries');
      
      // Pipeline name mapping
      const pipelineNames: Record<string, string> = {
        'agents': 'Agents',
        'atm': 'ATM',
        'balance_with_bot': 'Balance with BOT',
        'balance_with_mnos': 'Balance with MNOs',
        'balance_with_other_banks': 'Balance with Other Banks',
        'loans': 'Loans',
        'agent_transactions': 'Agent Transactions',
        'mobile_banking': 'Mobile Banking',
        'outgoing_fund_transfer': 'Outgoing Fund Transfer',
        'cash': 'Cash Information',
        'cards': 'Cards',
        'personal_data': 'Personal Data',
        'card_product': 'Card Product',
        'account_product_category': 'Account Product Category',
      };

      const tables: any[] = [];
      let totalRecords = 0;

      // Check if summaries directory exists
      if (fs.existsSync(summariesDir)) {
        const files = fs.readdirSync(summariesDir).filter(f => f.endsWith('.sql'));
        
        for (const file of files) {
          const pipelineName = file.replace('.sql', '');
          const displayName = pipelineNames[pipelineName] || pipelineName;
          
          try {
            const sqlPath = path.join(summariesDir, file);
            const sql = fs.readFileSync(sqlPath, 'utf-8');
            
            const result = await new Promise<any[]>((resolve, reject) => {
              connection.query(sql, (err: any, data: any[]) => {
                if (err) reject(err);
                else resolve(data);
              });
            });
            
            const recordCount = result.length > 0 ? parseInt(result[0].record_count, 10) : 0;
            
            tables.push({
              name: pipelineName,
              displayName: displayName,
              recordCount: recordCount,
              schema: 'pipeline',
              remarks: `Pipeline: ${displayName}`,
            });
            
            totalRecords += recordCount;
            
            this.logger.log(`📊 ${displayName}: ${recordCount.toLocaleString()} records`);
          } catch (err: any) {
            this.logger.warn(`Could not get count for ${pipelineName}: ${err.message}`);
            tables.push({
              name: pipelineName,
              displayName: pipelineNames[pipelineName] || pipelineName,
              recordCount: 0,
              schema: 'pipeline',
              remarks: `Pipeline: ${pipelineNames[pipelineName] || pipelineName}`,
            });
          }
        }
      } else {
        this.logger.warn(`Summaries directory not found: ${summariesDir}`);
      }

      connection.close();
      this.logger.log(`📊 DB2: Found ${tables.length} pipelines with ${totalRecords.toLocaleString()} total records`);
      return tables;
    } catch (error: any) {
      this.logger.error(`❌ DB2 Connection Failed: ${error.message}`);
      this.logger.error(`   Host: ${this.configService.get<string>('DB_HOST')}:${this.configService.get<string>('DB_PORT')}`);
      this.logger.error(`   Database: ${this.configService.get<string>('DB_NAME')}`);
      this.logger.error(`   User: ${this.configService.get<string>('DB_USERNAME')}`);
      throw error;
    }
  }

  async getPostgresTables(): Promise<any[]> {
    try {
      const pg = await import('pg');
      const { Client } = pg;
      const dbConfig = this.configService.getPgConfig();
      
      this.logger.log(`🔌 PostgreSQL: Connecting to ${dbConfig.host}:${dbConfig.port}/${dbConfig.database} as ${dbConfig.username}...`);
      
      const client = new Client({
        host: dbConfig.host,
        port: dbConfig.port,
        database: dbConfig.database,
        user: dbConfig.username,
        password: dbConfig.password,
      });

      await client.connect();
      
      this.logger.log(`✅ PostgreSQL: Connected successfully`);

      // Get tables with row counts - use case-insensitive comparison
      const result = await client.query(`
        SELECT 
          t.table_name,
          t.table_schema,
          obj_description((t.table_schema || '.' || t.table_name)::regclass, 'pg_class') as remarks,
          COALESCE(
            (SELECT COUNT(*) FROM information_schema.columns c WHERE LOWER(c.table_schema) = LOWER(t.table_schema) AND LOWER(c.table_name) = LOWER(t.table_name)),
            0
          ) as column_count
        FROM information_schema.tables t
        WHERE t.table_type = 'BASE TABLE'
        AND LOWER(t.table_schema) NOT IN ('pg_catalog', 'information_schema')
        ORDER BY t.table_name
      `);

      // Get row counts for each table
      const tables = await Promise.all(
        result.rows.map(async (row) => {
          try {
            // Use the actual table name from the result for counting - quote properly to preserve case
            const countResult = await client.query(`SELECT COUNT(*) as count FROM "${row.table_schema}"."${row.table_name}"`);
            return {
              name: row.table_name,
              schema: row.table_schema,
              recordCount: parseInt(countResult.rows[0].count, 10),
              columnCount: row.column_count,
              remarks: row.remarks,
            };
          } catch (countError: any) {
            this.logger.warn(`Could not get count for table ${row.table_schema}.${row.table_name}: ${countError.message}`);
            return {
              name: row.table_name,
              schema: row.table_schema,
              recordCount: 0,
              columnCount: row.column_count,
              remarks: row.remarks,
            };
          }
        })
      );

      await client.end();
      this.logger.log(`📊 PostgreSQL: Found ${tables.length} tables with ${tables.reduce((s, t) => s + t.recordCount, 0).toLocaleString()} total records`);
      return tables;
    } catch (error: any) {
      this.logger.error(`❌ PostgreSQL Connection Failed: ${error.message}`);
      this.logger.error(`   Host: ${this.configService.get<string>('PG_HOST')}:${this.configService.get<string>('PG_PORT')}`);
      this.logger.error(`   Database: ${this.configService.get<string>('PG_DATABASE')}`);
      this.logger.error(`   User: ${this.configService.get<string>('PG_USER')}`);
      return [];
    }
  }

  async getPostgresTableData(tableName: string, limit = 100, offset = 0): Promise<{ data: any[]; total: number; columns: string[] }> {
    try {
      const pg = await import('pg');
      const { Client } = pg;
      const dbConfig = this.configService.getPgConfig();
      
      const client = new Client({
        host: dbConfig.host,
        port: dbConfig.port,
        database: dbConfig.database,
        user: dbConfig.username,
        password: dbConfig.password,
      });

      await client.connect();

      // Check if table exists (case-insensitive)
      const checkResult = await client.query(`
        SELECT EXISTS (
          SELECT FROM information_schema.tables 
          WHERE LOWER(table_schema) = LOWER(SPLIT_PART($1, '.', 1)) 
            AND LOWER(table_name) = LOWER(SPLIT_PART($1, '.', 2))
        )
      `, [tableName]);

      if (!checkResult.rows[0].exists) {
        await client.end();
        this.logger.warn(`Table ${tableName} does not exist`);
        return { data: [], total: 0, columns: [] };
      }

      // Get column names (case-insensitive)
      const columnsResult = await client.query(`
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE LOWER(table_schema) = LOWER(SPLIT_PART($1, '.', 1)) 
          AND LOWER(table_name) = LOWER(SPLIT_PART($1, '.', 2))
        ORDER BY ordinal_position
      `, [tableName]);

      const columns = columnsResult.rows.map(row => row.column_name);

      if (columnsResult.rows.length === 0) {
        await client.end();
        return { data: [], total: 0, columns: [] };
      }

      // Get actual schema and table name from result (preserves case)
      const actualSchema = columnsResult.rows[0].table_schema;
      const actualTable = columnsResult.rows[0].table_name;

      // Get data - use actual table name with proper quoting
      const dataResult = await client.query(`SELECT * FROM "${actualSchema}"."${actualTable}" ORDER BY 1 LIMIT $1 OFFSET $2`, [limit, offset]);

      // Get total count - use actual table name with proper quoting
      const countResult = await client.query(`SELECT COUNT(*) as count FROM "${actualSchema}"."${actualTable}"`);

      await client.end();

      return {
        data: dataResult.rows,
        total: parseInt(countResult.rows[0].count, 10),
        columns,
      };
    } catch (error: any) {
      this.logger.error(`Error fetching table data: ${error.message}`);
      return { data: [], total: 0, columns: [] };
    }
  }

  async getDb2TableData(pipelineName: string, limit = 100, offset = 0): Promise<{ data: any[]; total: number; columns: string[] }> {
    try {
      const { ibmdb, connStr } = this.getDb2Connection();
      
      const connection = await ibmdb.open(connStr);

      // Map pipeline names to SQL files
      const sqlFileMap: Record<string, string> = {
        'agents': 'agents-from-agents-list-NEW-V5.table.sql',
        'atm': 'atm.sql',
        'balance_with_bot': 'balances-bot-v1.sql',
        'balance_with_mnos': 'balances-with-mnos.sql',
        'balance_with_other_banks': 'balance-with-other-bank-v1.sql',
        'loans': 'loan-information-v6.sql',
        'agent_transactions': 'agent-transactions-v2.sql',
        'mobile_banking': 'mobile-banking-v1.sql',
        'outgoing_fund_transfer': 'outgoing-fund-transfer-v1.sql',
        'cash': 'cash-information.sql',
        'cards': 'card_information.sql',
        'personal_data': 'personal_data_information-v4.sql',
        'card_product': 'card-product.sql',
        'account_product_category': 'account-product-category.sql',
      };

      const sqlFile = sqlFileMap[pipelineName];
      if (!sqlFile) {
        connection.close();
        this.logger.warn(`No SQL file found for pipeline: ${pipelineName}`);
        return { data: [], total: 0, columns: [] };
      }

      // Read the SQL file - go up one level from backend to project root
      const pipelinesDir = this.configService.get<string>('PIPELINES_DIR') || '..';
      const sqlPath = path.resolve(process.cwd(), pipelinesDir, 'sqls', sqlFile);
      
      if (!fs.existsSync(sqlPath)) {
        connection.close();
        this.logger.warn(`SQL file not found: ${sqlPath}`);
        return { data: [], total: 0, columns: [] };
      }

      let sql = fs.readFileSync(sqlPath, 'utf-8');

      // Get total count first
      const countSql = `SELECT COUNT(*) as count FROM (${sql})`;
      const countResult = await new Promise<any[]>((resolve, reject) => {
        connection.query(countSql, (err: any, data: any[]) => {
          if (err) reject(err);
          else resolve(data);
        });
      });

      const total = parseInt(countResult[0].count, 10);

      // Add pagination to the query
      const paginatedSql = `${sql} ORDER BY 1 OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY`;
      
      // Get data with pagination
      const dataResult = await new Promise<any[]>((resolve, reject) => {
        connection.query(paginatedSql, (err: any, data: any[]) => {
          if (err) reject(err);
          else resolve(data);
        });
      });

      // Get columns from first row
      const columns = dataResult.length > 0 ? Object.keys(dataResult[0]) : [];

      connection.close();

      this.logger.log(`📥 ${pipelineName}: ${dataResult.length} rows (${offset}-${offset + limit}) of ${total} total`);

      return {
        data: dataResult,
        total,
        columns,
      };
    } catch (error: any) {
      this.logger.error(`Error fetching DB2 pipeline data for ${pipelineName}: ${error.message}`);
      return { data: [], total: 0, columns: [] };
    }
  }

  async getPipelineStats(): Promise<any> {
    try {
      const pg = await import('pg');
      const { Client } = pg;
      const dbConfig = this.configService.getPgConfig();
      
      const client = new Client({
        host: dbConfig.host,
        port: dbConfig.port,
        database: dbConfig.database,
        user: dbConfig.username,
        password: dbConfig.password,
      });

      await client.connect();

      // Get pipeline state data
      const result = await client.query(`
        SELECT 
          pipeline_name,
          last_run,
          last_run_status,
          records_processed,
          last_successful_run,
          error_message
        FROM pipeline_state
        ORDER BY pipeline_name
      `);

      await client.end();

      return result.rows.map(row => ({
        name: row.pipeline_name,
        lastRun: row.last_run,
        lastRunStatus: row.last_run_status,
        recordsProcessed: parseInt(row.records_processed, 10),
        lastSuccessfulRun: row.last_successful_run,
        errorMessage: row.error_message,
      }));
    } catch (error: any) {
      this.logger.error(`Error fetching pipeline stats: ${error.message}`);
      return [];
    }
  }
}