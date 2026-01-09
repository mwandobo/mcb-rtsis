#!/usr/bin/env python3
"""
Verify Data Pipeline - Check if data has been successfully piped to PostgreSQL
"""

import psycopg2
import logging
from config import Config

def verify_data_pipeline():
    """Verify that data has been successfully piped to PostgreSQL"""
    config = Config()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.database.pg_host,
            port=config.database.pg_port,
            database=config.database.pg_database,
            user=config.database.pg_user,
            password=config.database.pg_password
        )
        
        cursor = conn.cursor()
        
        logger.info("🔍 VERIFYING DATA PIPELINE RESULTS")
        logger.info("=" * 60)
        
        # List of tables to check
        tables_to_check = [
            'microfinanceSegmentLoans',
            'balanceWithOtherBank',
            'balancesBot',
            'overdraft',
            'cardTransaction',
            'personalDataInformation',
            'digitalCredit',
            'atmTransaction',
            'balanceWithMnos',
            'chequeClearing',
            'bankerChequesDrafts',
            'incomeStatement',
            'interBankLoanReceivable',
            'icbmTransaction',
            'investmentDebtSecurities'
        ]
        
        total_records = 0
        
        for table_name in tables_to_check:
            try:
                # Check if table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    );
                """, (table_name,))
                
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    # Get record count
                    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}";')
                    count = cursor.fetchone()[0]
                    total_records += count
                    
                    logger.info(f"✅ {table_name}: {count:,} records")
                    
                    # Show sample data
                    cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 3;')
                    sample_data = cursor.fetchall()
                    
                    if sample_data:
                        # Get column names
                        cursor.execute("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name = %s 
                            ORDER BY ordinal_position
                            LIMIT 5;
                        """, (table_name,))
                        columns = [row[0] for row in cursor.fetchall()]
                        
                        logger.info(f"   📋 Sample columns: {', '.join(columns)}")
                        logger.info(f"   📊 Sample record: {sample_data[0][:5]}...")
                    
                else:
                    logger.warning(f"⚠️ {table_name}: Table does not exist")
                    
            except Exception as e:
                logger.error(f"❌ Error checking {table_name}: {e}")
        
        logger.info("=" * 60)
        logger.info(f"📊 TOTAL RECORDS ACROSS ALL TABLES: {total_records:,}")
        
        # Check recent data
        logger.info("\n🕒 CHECKING RECENT DATA (2024):")
        logger.info("-" * 40)
        
        # Check microfinance loans from 2024
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "microfinanceSegmentLoans" 
                WHERE "disbursementDate" LIKE '2024%'
            """)
            mf_2024_count = cursor.fetchone()[0]
            logger.info(f"💰 Microfinance loans from 2024: {mf_2024_count:,}")
        except:
            logger.info("💰 Microfinance loans from 2024: N/A")
        
        # Check balance with other banks from 2024
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "balanceWithOtherBank" 
                WHERE "transactionDate" >= '2024-01-01'
            """)
            bob_2024_count = cursor.fetchone()[0]
            logger.info(f"🏦 Balance with other banks from 2024: {bob_2024_count:,}")
        except:
            logger.info("🏦 Balance with other banks from 2024: N/A")
        
        # Check balances with BOT from 2024
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "balancesBot" 
                WHERE "transactionDate" >= '2024-01-01'
            """)
            bot_2024_count = cursor.fetchone()[0]
            logger.info(f"🏛️ Balances with BOT from 2024: {bot_2024_count:,}")
        except:
            logger.info("🏛️ Balances with BOT from 2024: N/A")
        
        # Check overdraft records
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "overdraft"
            """)
            overdraft_count = cursor.fetchone()[0]
            logger.info(f"📈 Overdraft records: {overdraft_count:,}")
        except:
            logger.info("📈 Overdraft records: N/A")
        
        # Check digital credit records
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "digitalCredit"
                WHERE "loanDisbursementDate" >= '2020-01-01'
            """)
            digital_credit_count = cursor.fetchone()[0]
            logger.info(f"💳 Digital credit records from 2020: {digital_credit_count:,}")
        except:
            logger.info("💳 Digital credit records from 2020: N/A")
        
        # Check ATM transaction records
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "atmTransaction"
                WHERE "transactionDate" >= '2024-01-01'
            """)
            atm_transaction_count = cursor.fetchone()[0]
            logger.info(f"🏧 ATM transaction records from 2024: {atm_transaction_count:,}")
        except:
            logger.info("🏧 ATM transaction records from 2024: N/A")
        
        # Check balance with MNOs records
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "balanceWithMnos"
                WHERE "reportingDate" >= '2024-01-01'
            """)
            balance_mnos_count = cursor.fetchone()[0]
            logger.info(f"📱 Balance with MNOs records from 2024: {balance_mnos_count:,}")
        except:
            logger.info("📱 Balance with MNOs records from 2024: N/A")
        
        # Check cheque clearing records
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "chequeClearing"
                WHERE "transactionDate" >= '2024-01-01'
            """)
            cheque_clearing_count = cursor.fetchone()[0]
            logger.info(f"💰 Cheque clearing records from 2024: {cheque_clearing_count:,}")
        except:
            logger.info("💰 Cheque clearing records from 2024: N/A")
        
        # Banker cheques and drafts from 2024
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "bankerChequesDrafts"
                WHERE "transactionDate" >= '2024-01-01'
            """)
            banker_cheques_count = cursor.fetchone()[0]
            logger.info(f"🏦 Banker cheques and drafts from 2024: {banker_cheques_count:,}")
        except:
            logger.info("🏦 Banker cheques and drafts from 2024: N/A")
        
        # Income statement records
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "incomeStatement"
            """)
            income_statement_count = cursor.fetchone()[0]
            logger.info(f"📊 Income statement records: {income_statement_count:,}")
        except:
            logger.info("📊 Income statement records: N/A")
        
        # Inter-bank loan receivable from 2024
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "interBankLoanReceivable"
                WHERE "contractDate" >= '2024-01-01'
            """)
            inter_bank_loan_count = cursor.fetchone()[0]
            logger.info(f"🏦 Inter-bank loan receivable from 2024: {inter_bank_loan_count:,}")
        except:
            logger.info("🏦 Inter-bank loan receivable from 2024: N/A")
        
        # ICBM transactions from 2024
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "icbmTransaction"
                WHERE "transactionDate" >= '2024-01-01'
            """)
            icbm_transaction_count = cursor.fetchone()[0]
            logger.info(f"💱 ICBM transactions from 2024: {icbm_transaction_count:,}")
        except:
            logger.info("💱 ICBM transactions from 2024: N/A")
        
        # Investment debt securities from 2024
        try:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM "investmentDebtSecurities"
                WHERE "reportingDate" >= '2024-01-01'
            """)
            investment_debt_count = cursor.fetchone()[0]
            logger.info(f"📈 Investment debt securities from 2024: {investment_debt_count:,}")
        except:
            logger.info("📈 Investment debt securities from 2024: N/A")
        
        logger.info("\n💱 CURRENCY BREAKDOWN:")
        logger.info("-" * 30)
        
        # Microfinance by currency
        try:
            cursor.execute("""
                SELECT "currency", COUNT(*), SUM("orgOutstandingPrincipalAmount")
                FROM "microfinanceSegmentLoans" 
                GROUP BY "currency"
                ORDER BY COUNT(*) DESC
            """)
            mf_currency = cursor.fetchall()
            for currency, count, total_amount in mf_currency:
                logger.info(f"💰 Microfinance {currency}: {count:,} loans, Total: {total_amount:,.2f}")
        except:
            pass
        
        # Balance with other banks by currency
        try:
            cursor.execute("""
                SELECT "currency", COUNT(*), SUM("orgAmount")
                FROM "balanceWithOtherBank" 
                GROUP BY "currency"
                ORDER BY COUNT(*) DESC
            """)
            bob_currency = cursor.fetchall()
            for currency, count, total_amount in bob_currency:
                logger.info(f"🏦 Other banks {currency}: {count:,} records, Total: {total_amount:,.2f}")
        except:
            pass
        
        # Balances with BOT by currency
        try:
            cursor.execute("""
                SELECT "currency", COUNT(*), SUM("orgAmount")
                FROM "balancesBot" 
                GROUP BY "currency"
                ORDER BY COUNT(*) DESC
            """)
            bot_currency = cursor.fetchall()
            for currency, count, total_amount in bot_currency:
                logger.info(f"🏛️ BOT balances {currency}: {count:,} records, Total: {total_amount:,.2f}")
        except:
            pass
        
        # Digital credit by facilitator
        try:
            cursor.execute("""
                SELECT "servicesFacilitator", COUNT(*), SUM("tzsLoanBalance")
                FROM "digitalCredit" 
                GROUP BY "servicesFacilitator"
                ORDER BY COUNT(*) DESC
            """)
            dc_facilitator = cursor.fetchall()
            for facilitator, count, total_balance in dc_facilitator:
                logger.info(f"💳 Digital credit {facilitator}: {count:,} loans, Balance: {total_balance:,.2f}")
        except:
            pass
        
        # ATM transactions by nature
        try:
            cursor.execute("""
                SELECT "transactionNature", COUNT(*), SUM("orgTransactionAmount")
                FROM "atmTransaction" 
                GROUP BY "transactionNature"
                ORDER BY COUNT(*) DESC
            """)
            atm_nature = cursor.fetchall()
            for nature, count, total_amount in atm_nature:
                logger.info(f"🏧 ATM {nature}: {count:,} transactions, Total: {total_amount:,.2f}")
        except:
            pass
        
        # Balance with MNOs by operator
        try:
            cursor.execute("""
                SELECT "mnoCode", COUNT(*), SUM("orgFloatAmount")
                FROM "balanceWithMnos" 
                GROUP BY "mnoCode"
                ORDER BY COUNT(*) DESC
            """)
            mnos_operator = cursor.fetchall()
            for operator, count, total_amount in mnos_operator:
                logger.info(f"📱 MNO {operator}: {count:,} records, Float: {total_amount:,.2f}")
        except:
            pass
        
        # Cheque clearing by currency
        try:
            cursor.execute("""
                SELECT "currency", COUNT(*), SUM("orgAmountPayment")
                FROM "chequeClearing" 
                GROUP BY "currency"
                ORDER BY COUNT(*) DESC
            """)
            cheque_currency = cursor.fetchall()
            for currency, count, total_amount in cheque_currency:
                logger.info(f"💰 Cheque clearing {currency}: {count:,} cheques, Total: {total_amount:,.2f}")
        except:
            pass
        
        # Banker cheques and drafts currency breakdown
        try:
            cursor.execute("""
                SELECT "currency", COUNT(*), SUM("orgAmount")
                FROM "bankerChequesDrafts" 
                GROUP BY "currency"
                ORDER BY COUNT(*) DESC
            """)
            banker_cheques_currency = cursor.fetchall()
            for currency, count, total_amount in banker_cheques_currency:
                logger.info(f"🏦 Banker cheques/drafts {currency}: {count:,} items, Total: {total_amount:,.2f}")
        except:
            pass
        
        # Income statement financial summary
        try:
            cursor.execute("""
                SELECT "interestIncome", "interestExpense", "nonInterestIncome", "nonInterestExpenses"
                FROM "incomeStatement" 
                ORDER BY "reportingDate" DESC
                LIMIT 1
            """)
            income_data = cursor.fetchone()
            if income_data:
                interest_income, interest_expense, non_interest_income, non_interest_expenses = income_data
                net_interest = (interest_income or 0) - (interest_expense or 0)
                logger.info(f"📊 Interest Income: {interest_income:,.2f} | Interest Expense: {interest_expense:,.2f}")
                logger.info(f"📊 Net Interest Income: {net_interest:,.2f}")
                logger.info(f"📊 Non-Interest Income: {non_interest_income:,.2f} | Non-Interest Expenses: {non_interest_expenses:,.2f}")
        except:
            pass
        
        # Inter-bank loan receivable summary
        try:
            cursor.execute("""
                SELECT "currency", COUNT(*), SUM("orgOutstandingPrincipalAmount")
                FROM "interBankLoanReceivable" 
                GROUP BY "currency"
                ORDER BY COUNT(*) DESC
            """)
            loan_currency = cursor.fetchall()
            for currency, count, total_amount in loan_currency:
                logger.info(f"🏦 Inter-bank loans {currency}: {count:,} loans, Outstanding: {total_amount:,.2f}")
        except:
            pass
        
        # ICBM transaction summary
        try:
            cursor.execute("""
                SELECT "transactionType", COUNT(*), SUM("tzsAmount")
                FROM "icbmTransaction" 
                GROUP BY "transactionType"
                ORDER BY COUNT(*) DESC
            """)
            icbm_types = cursor.fetchall()
            for transaction_type, count, total_amount in icbm_types:
                logger.info(f"💱 ICBM {transaction_type}: {count:,} transactions, Total: {total_amount:,.2f} TZS")
        except:
            pass
        
        # Investment debt securities summary
        try:
            cursor.execute("""
                SELECT "securityType", COUNT(*), SUM("orgCostValueAmount")
                FROM "investmentDebtSecurities" 
                GROUP BY "securityType"
                ORDER BY COUNT(*) DESC
            """)
            security_types = cursor.fetchall()
            for security_type, count, total_cost in security_types:
                logger.info(f"📈 {security_type}: {count:,} securities, Cost: {total_cost:,.2f}")
        except:
            pass
        
        # Investment debt securities by currency
        try:
            cursor.execute("""
                SELECT "currency", COUNT(*), SUM("orgCostValueAmount")
                FROM "investmentDebtSecurities" 
                GROUP BY "currency"
                ORDER BY COUNT(*) DESC
            """)
            security_currency = cursor.fetchall()
            for currency, count, total_cost in security_currency:
                logger.info(f"📈 Investment securities {currency}: {count:,} securities, Cost: {total_cost:,.2f}")
        except:
            pass
        
        logger.info("\n🎉 DATA PIPELINE VERIFICATION COMPLETE!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to verify data pipeline: {e}")
        logger.error("   This is likely due to PostgreSQL connection issues")
        logger.error("   Contact your DBA to resolve pg_hba.conf configuration")
        raise

if __name__ == "__main__":
    verify_data_pipeline()