import logging
from typing import Dict, Any, Optional

import psycopg2

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, postgres_url: str):
        """Initialize database connection"""
        try:
            self.conn = psycopg2.connect(postgres_url)
            logger.info("connected")
        except Exception as e:
            logger.error(f"connection failed: {e}")
            raise
    
    def get_user_stats(self, user_id: str, days: int = 30) -> Optional[Dict[str, Any]]:
       cursor = self.conn.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    AVG(amount) as avg_amount,
                    COUNT(*) as txn_count,
                    MAX(amount) as max_amount,
                    MIN(amount) as min_amount
                FROM transactions
                WHERE user_id = %s
                AND timestamp > NOW() - INTERVAL '%s days'
            """, (user_id, days))
            
            result = cursor.fetchone()
            
            if result:
                avg_amount, txn_count, max_amount, min_amount = result
                return {
                    'avg_amount': float(avg_amount) if avg_amount else 0.0,
                    'txn_count': int(txn_count) if txn_count else 0,
                    'max_amount': float(max_amount) if max_amount else 0.0,
                    'min_amount': float(min_amount) if min_amount else 0.0,
                }
            
            return None
        
        except Exception as e:
            logger.error(f"Failed to get user stats: {e}")
            return None
        
        finally:
            cursor.close()
    
    def get_account_age(self, user_id: str) -> Optional[int]:
       cursor = self.conn.cursor()
        
        try:
            cursor.execute("""
                SELECT EXTRACT(DAY FROM NOW() - created_at)
                FROM users
                WHERE user_id = %s
            """, (user_id,))
            
            result = cursor.fetchone()
            return int(result[0]) if result and result[0] else 0
        
        except Exception as e:
            logger.error(f"Failed to get account age: {e}")
            return 0
        
        finally:
            cursor.close()
    
    def close(self):
        try:
            self.conn.close()
            logger.info("PostgreSQL connection closed")
        except Exception as e:
            logger.error(f"Failed to close PostgreSQL: {e}")
