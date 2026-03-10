"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var DatabasesService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.DatabasesService = void 0;
const common_1 = require("@nestjs/common");
const config_service_1 = require("../config/config.service");
const fs = require("fs");
const path = require("path");
let DatabasesService = DatabasesService_1 = class DatabasesService {
    constructor(configService) {
        this.configService = configService;
        this.logger = new common_1.Logger(DatabasesService_1.name);
        this.db2TablesCache = null;
        this.CACHE_TTL = 5 * 60 * 1000;
    }
    getDb2Connection() {
        const ibmdb = require('ibm_db');
        const dbConfig = this.configService.getDatabaseConfig();
        const connStr = `DATABASE=${dbConfig.database};HOSTNAME=${dbConfig.host};PORT=${dbConfig.port};PROTOCOL=TCPIP;UID=${dbConfig.username};PWD=${dbConfig.password};`;
        return { ibmdb, connStr };
    }
    async getDb2Tables() {
        if (this.db2TablesCache && Date.now() - this.db2TablesCache.timestamp < this.CACHE_TTL) {
            this.logger.log(`📦 Returning cached DB2 tables (${this.db2TablesCache.data.length} tables)`);
            return this.db2TablesCache.data;
        }
        try {
            const { ibmdb, connStr } = this.getDb2Connection();
            this.logger.log(`🔌 DB2: Connecting to ${this.configService.get('DB_HOST')}:${this.configService.get('DB_PORT')}/${this.configService.get('DB_NAME')} as ${this.configService.get('DB_USERNAME')}...`);
            const connection = await ibmdb.open(connStr);
            this.logger.log(`✅ DB2: Connected successfully`);
            let summariesDir = path.resolve(process.cwd(), '..', 'sqls', 'db2-summaries');
            if (!fs.existsSync(summariesDir)) {
                summariesDir = path.resolve(process.cwd(), 'sqls', 'db2-summaries');
            }
            if (!fs.existsSync(summariesDir)) {
                summariesDir = path.resolve('sqls', 'db2-summaries');
            }
            const pipelineNames = {
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
                'account_information': 'Account Information',
            };
            const tables = [];
            let totalRecords = 0;
            if (fs.existsSync(summariesDir)) {
                const files = fs.readdirSync(summariesDir).filter(f => f.endsWith('.sql'));
                for (const file of files) {
                    const pipelineName = file.replace('.sql', '');
                    const displayName = pipelineNames[pipelineName] || pipelineName;
                    try {
                        const sqlPath = path.join(summariesDir, file);
                        const sql = fs.readFileSync(sqlPath, 'utf-8');
                        const result = await new Promise((resolve, reject) => {
                            connection.query(sql, (err, data) => {
                                if (err)
                                    reject(err);
                                else
                                    resolve(data);
                            });
                        });
                        const recordCount = result.length > 0 ? parseInt(result[0].RECORD_COUNT || result[0].record_count, 10) : 0;
                        tables.push({
                            name: pipelineName,
                            displayName: displayName,
                            recordCount: isNaN(recordCount) ? 0 : recordCount,
                            schema: 'pipeline',
                            remarks: `Pipeline: ${displayName}`,
                        });
                        totalRecords += isNaN(recordCount) ? 0 : recordCount;
                        this.logger.log(`📊 ${displayName}: ${isNaN(recordCount) ? 0 : recordCount.toLocaleString()} records`);
                    }
                    catch (err) {
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
            }
            else {
                this.logger.warn(`Summaries directory not found: ${summariesDir}`);
            }
            connection.close();
            this.logger.log(`📊 DB2: Found ${tables.length} pipelines with ${totalRecords.toLocaleString()} total records`);
            this.db2TablesCache = {
                data: tables,
                timestamp: Date.now(),
            };
            this.logger.log(`💾 Cached DB2 tables for ${this.CACHE_TTL / 1000 / 60} minutes`);
            return tables;
        }
        catch (error) {
            this.logger.error(`❌ DB2 Connection Failed: ${error.message}`);
            this.logger.error(`   Host: ${this.configService.get('DB_HOST')}:${this.configService.get('DB_PORT')}`);
            this.logger.error(`   Database: ${this.configService.get('DB_NAME')}`);
            this.logger.error(`   User: ${this.configService.get('DB_USERNAME')}`);
            throw error;
        }
    }
    async getPostgresTables() {
        try {
            const pg = await Promise.resolve().then(() => require('pg'));
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
            const tables = await Promise.all(result.rows.map(async (row) => {
                try {
                    const countResult = await client.query(`SELECT COUNT(*) as count FROM "${row.table_schema}"."${row.table_name}"`);
                    return {
                        name: row.table_name,
                        schema: row.table_schema,
                        recordCount: parseInt(countResult.rows[0].count, 10),
                        columnCount: row.column_count,
                        remarks: row.remarks,
                    };
                }
                catch (countError) {
                    this.logger.warn(`Could not get count for table ${row.table_schema}.${row.table_name}: ${countError.message}`);
                    return {
                        name: row.table_name,
                        schema: row.table_schema,
                        recordCount: 0,
                        columnCount: row.column_count,
                        remarks: row.remarks,
                    };
                }
            }));
            await client.end();
            this.logger.log(`📊 PostgreSQL: Found ${tables.length} tables with ${tables.reduce((s, t) => s + t.recordCount, 0).toLocaleString()} total records`);
            return tables;
        }
        catch (error) {
            this.logger.error(`❌ PostgreSQL Connection Failed: ${error.message}`);
            this.logger.error(`   Host: ${this.configService.get('PG_HOST')}:${this.configService.get('PG_PORT')}`);
            this.logger.error(`   Database: ${this.configService.get('PG_DATABASE')}`);
            this.logger.error(`   User: ${this.configService.get('PG_USER')}`);
            return [];
        }
    }
    async getPostgresTableData(tableName, limit = 100, offset = 0) {
        try {
            const pg = await Promise.resolve().then(() => require('pg'));
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
            const actualSchema = columnsResult.rows[0].table_schema;
            const actualTable = columnsResult.rows[0].table_name;
            const dataResult = await client.query(`SELECT * FROM "${actualSchema}"."${actualTable}" ORDER BY 1 LIMIT $1 OFFSET $2`, [limit, offset]);
            const countResult = await client.query(`SELECT COUNT(*) as count FROM "${actualSchema}"."${actualTable}"`);
            await client.end();
            return {
                data: dataResult.rows,
                total: parseInt(countResult.rows[0].count, 10),
                columns,
            };
        }
        catch (error) {
            this.logger.error(`Error fetching table data: ${error.message}`);
            return { data: [], total: 0, columns: [] };
        }
    }
    async getDb2TableData(pipelineName, limit = 10, offset = 0) {
        try {
            const { ibmdb, connStr } = this.getDb2Connection();
            const connection = await ibmdb.open(connStr);
            const sqlFileMap = {
                'agents': 'agents-from-agents-list-NEW-V5.table.sql',
                'atm': 'atm.sql',
                'balance_with_bot': 'balances-bot-v1.sql',
                'balance_with_mnos': 'balances-with-mnos.sql',
                'balance_with_other_banks': 'balance-with-other-bank-v1.sql',
                'loans': 'loan-information-v7.sql',
                'agent_transactions': 'agent-transactions-v2.sql',
                'mobile_banking': 'mobile-banking-v1.sql',
                'outgoing_fund_transfer': 'outgoing-fund-transfer-v1.sql',
                'cash': 'cash-information.sql',
                'cards': 'card_information.sql',
                'personal_data': 'personal_data_information-v4.sql',
                'card_product': 'card-product.sql',
                'account_product_category': 'account-product-category.sql',
                'account_information': 'account-information.sql',
            };
            const sqlFile = sqlFileMap[pipelineName];
            if (!sqlFile) {
                connection.close();
                this.logger.warn(`No SQL file found for pipeline: ${pipelineName}`);
                return { data: [], total: 0, columns: [] };
            }
            const pipelinesDir = this.configService.get('PIPELINES_DIR') || '..';
            let sqlPath = path.resolve(process.cwd(), pipelinesDir, 'sqls', sqlFile);
            if (!fs.existsSync(sqlPath)) {
                sqlPath = path.resolve(process.cwd(), '..', 'sqls', sqlFile);
            }
            if (!fs.existsSync(sqlPath)) {
                sqlPath = path.resolve('sqls', sqlFile);
            }
            if (!fs.existsSync(sqlPath)) {
                connection.close();
                this.logger.warn(`SQL file not found: ${sqlPath}`);
                return { data: [], total: 0, columns: [] };
            }
            let sql = fs.readFileSync(sqlPath, 'utf-8');
            sql = sql.replace(/:last_timestamp/g, "'1900-01-01 00:00:00'");
            sql = sql.trim().replace(/;$/, '');
            const hasGroupBy = /GROUP\s+BY/i.test(sql);
            const hasWithClause = /^\s*WITH\s+/i.test(sql);
            const hasInlineCTE = /\)\s+\w+\s+AS\s*\(/i.test(sql);
            const hasOrderBy = /ORDER\s+BY/i.test(sql);
            const hasRowNumber = /ROW_NUMBER\s*\(\s*OVER\s*\(/i.test(sql);
            let paginatedSql;
            let countSql;
            if (hasWithClause || hasInlineCTE || hasRowNumber) {
                const allData = await new Promise((resolve, reject) => {
                    connection.query(sql, (err, data) => {
                        if (err) {
                            this.logger.error(`Query failed for ${pipelineName}: ${err.message}`);
                            reject(err);
                        }
                        else {
                            resolve(data);
                        }
                    });
                });
                const total = allData.length;
                const paginatedData = allData.slice(offset, offset + limit);
                const columns = paginatedData.length > 0 ? Object.keys(paginatedData[0]) : [];
                connection.close();
                this.logger.log(`📥 ${pipelineName}: ${paginatedData.length} rows (${offset}-${offset + limit}) of ${total} total (JS pagination)`);
                return {
                    data: paginatedData,
                    total,
                    columns,
                };
            }
            else if (hasGroupBy) {
                countSql = `SELECT COUNT(*) as count FROM (${sql})`;
                paginatedSql = `
          WITH base_query AS (
            ${sql}
          )
          SELECT * FROM (
            SELECT base_query.*, ROW_NUMBER() OVER (ORDER BY 1) as rn
            FROM base_query
          ) AS wrapped
          WHERE rn > ${offset} AND rn <= ${offset + limit}
        `.trim();
            }
            else {
                countSql = `SELECT COUNT(*) as count FROM (${sql})`;
                if (hasOrderBy) {
                    const sqlWithoutOrder = sql.replace(/ORDER\s+BY\s+[^)]+$/i, '');
                    paginatedSql = `${sqlWithoutOrder} OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY`;
                }
                else {
                    paginatedSql = `${sql} ORDER BY 1 OFFSET ${offset} ROWS FETCH NEXT ${limit} ROWS ONLY`;
                }
            }
            const countResult = await new Promise((resolve, reject) => {
                connection.query(countSql, (err, data) => {
                    if (err)
                        reject(err);
                    else
                        resolve(data);
                });
            });
            const total = parseInt(countResult[0]?.COUNT || countResult[0]?.count || '0', 10);
            const dataResult = await new Promise((resolve, reject) => {
                connection.query(paginatedSql, (err, data) => {
                    if (err) {
                        this.logger.error(`Pagination query failed for ${pipelineName}: ${err.message}`);
                        reject(err);
                    }
                    else {
                        resolve(data);
                    }
                });
            });
            const columns = dataResult.length > 0 ? Object.keys(dataResult[0]) : [];
            connection.close();
            this.logger.log(`📥 ${pipelineName}: ${dataResult.length} rows (${offset}-${offset + limit}) of ${total} total`);
            return {
                data: dataResult,
                total,
                columns,
            };
        }
        catch (error) {
            this.logger.error(`Error fetching DB2 pipeline data for ${pipelineName}: ${error.message}`);
            return { data: [], total: 0, columns: [] };
        }
    }
    async getPipelineStats() {
        try {
            const pg = await Promise.resolve().then(() => require('pg'));
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
        }
        catch (error) {
            this.logger.error(`Error fetching pipeline stats: ${error.message}`);
            return [];
        }
    }
    clearDb2Cache() {
        this.db2TablesCache = null;
        this.logger.log('🗑️  DB2 cache cleared');
    }
    getCacheInfo() {
        if (!this.db2TablesCache) {
            return { cached: false, ttl: this.CACHE_TTL };
        }
        const age = Date.now() - this.db2TablesCache.timestamp;
        return {
            cached: true,
            age,
            ttl: this.CACHE_TTL,
        };
    }
};
exports.DatabasesService = DatabasesService;
exports.DatabasesService = DatabasesService = DatabasesService_1 = __decorate([
    (0, common_1.Injectable)(),
    __metadata("design:paramtypes", [config_service_1.ConfigService])
], DatabasesService);
//# sourceMappingURL=databases.service.js.map