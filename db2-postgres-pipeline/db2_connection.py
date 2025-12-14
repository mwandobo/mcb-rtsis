"""
DB2 connection module using pyodbc
"""

import pyodbc
import logging
from contextlib import contextmanager
from config import Config

class DB2Connection:
    def __init__(self):
        self.config = Config()
        self.logger = logging.getLogger(__name__)
        
    def get_connection_string(self):
        """Build DB2 connection string for pyodbc"""
        return (
            f"DRIVER={{IBM DB2 ODBC DRIVER}};"
            f"DATABASE={self.config.database.db2_database};"
            f"HOSTNAME={self.config.database.db2_host};"
            f"PORT={self.config.database.db2_port};"
            f"PROTOCOL=TCPIP;"
            f"UID={self.config.database.db2_user};"
            f"PWD={self.config.database.db2_password};"
            f"CURRENTSCHEMA={self.config.database.db2_schema};"
        )
    
    @contextmanager
    def get_connection(self):
        """Get DB2 connection using pyodbc"""
        conn = None
        try:
            conn_str = self.get_connection_string()
            self.logger.debug(f"Connecting to DB2: {conn_str.replace(self.config.database.db2_password, '***')}")
            
            # Connect using pyodbc
            conn = pyodbc.connect(conn_str)
            
            self.logger.info("[OK] Connected to DB2")
            yield conn
            
        except Exception as e:
            self.logger.error(f"[ERROR] DB2 connection failed: {e}")
            raise
        finally:
            if conn:
                try:
                    conn.close()
                    self.logger.debug("DB2 connection closed")
                except:
                    pass
    
    def test_connection(self):
        """Test DB2 connection"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT CURRENT TIMESTAMP FROM SYSIBM.SYSDUMMY1")
                result = cursor.fetchone()
                self.logger.info(f"[OK] DB2 connection test successful: {result[0]}")
                return True
        except Exception as e:
            self.logger.error(f"[ERROR] DB2 connection test failed: {e}")
            return False
    
    def execute_query(self, query, params=None):
        """Execute query and return results"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                results = cursor.fetchall()
                return results
        except Exception as e:
            self.logger.error(f"[ERROR] Query execution failed: {e}")
            raise