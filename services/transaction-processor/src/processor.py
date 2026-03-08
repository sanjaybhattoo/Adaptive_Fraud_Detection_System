import json
import logging
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, Producer, KafkaError
from db import Database
from models import Transaction
from utils import parse_timestamp

logger = logging.getLogger(__name__)


class TransactionProcessor:
    def __init__(self, kafka_brokers: str = None, pg_url: str = None, mongo_url: str = None):
        """Initialize processor with Kafka and database connections"""
        import os
        
        self.kafka_brokers = kafka_brokers or os.getenv('KAFKA_BROKERS', 'localhost:9092')
        self.pg_url = pg_url or os.getenv('POSTGRES_URL')
        self.mongo_url = mongo_url or os.getenv('MONGO_URL')
        self.consumer_group = os.getenv('CONSUMER_GROUP', 'transaction-processor-group')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'transactions')
        
        # Initialize Kafka
        self.consumer = self._init_consumer()
        self.producer = self._init_producer()
        
        # Initialize databases
        self.db = Database(self.pg_url, self.mongo_url)
        
        # Statistics
        self.processed = 0
        self.errors = 0
        
        logger.info(f"Processor initialized - Kafka brokers: {self.kafka_brokers}")
    
    def _init_consumer(self) -> Consumer:
        """Initialize Kafka consumer"""
        brokers = self.kafka_brokers.split(',')
        
        config = {
            'bootstrap.servers': ','.join(brokers),
            'group.id': self.consumer_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'session.timeout.ms': 30000,
            'api.version.request.timeout.ms': 10000,
        }
        
        consumer = Consumer(config)
        consumer.subscribe([self.kafka_topic])
        logger.info(f"Kafka consumer created - topic: {self.kafka_topic}")
        
        return consumer
    
    def _init_producer(self) -> Producer:
        """Initialize Kafka producer"""
        brokers = self.kafka_brokers.split(',')
        
        config = {
            'bootstrap.servers': ','.join(brokers),
            'acks': 'all',
            'retries': 3,
        }
        
        logger.info("Kafka producer created")
        return Producer(config)
    
    def validate_transaction(self, txn: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate transaction schema
        Returns: (is_valid, error_message)
        """
        required_fields = [
            'transaction_id', 'user_id', 'card_id', 'merchant_id',
            'amount', 'timestamp', 'ip_address'
        ]
        
        # Check required fields
        for field in required_fields:
            if field not in txn:
                return False, f"Missing required field: {field}"
        
        # Validate amount
        try:
            amount = float(txn['amount'])
            if amount <= 0:
                return False, "Amount must be positive"
        except (ValueError, TypeError):
            return False, "Amount must be a number"
        
        # Validate timestamp format
        try:
            parse_timestamp(txn['timestamp'])
        except ValueError as e:
            return False, f"Invalid timestamp: {e}"
        
        return True, None
    
    def enrich_transaction(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich transaction with user and merchant profiles
        """
        user_id = txn['user_id']
        merchant_id = txn['merchant_id']
        
        # Lookup user profile
        user_profile = self.db.get_user_profile(user_id)
        if user_profile:
            from datetime import datetime
            account_age = (datetime.now() - user_profile.get('created_at', datetime.now())).days
            txn['user_profile'] = {
                'account_age_days': account_age,
                'total_transactions': user_profile.get('transaction_count', 0),
                'fraud_count': user_profile.get('fraud_count', 0),
            }
        else:
            txn['user_profile'] = None
            logger.debug(f"User profile not found: {user_id}")
        
        # Lookup merchant profile
        merchant_profile = self.db.get_merchant_profile(merchant_id)
        if merchant_profile:
            txn['merchant_profile'] = {
                'category': merchant_profile.get('category'),
                'monthly_volume': merchant_profile.get('monthly_volume', 0),
                'fraud_rate': merchant_profile.get('fraud_rate', 0),
            }
        else:
            txn['merchant_profile'] = None
            logger.debug(f"Merchant profile not found: {merchant_id}")
        
        return txn
    
    def save_transaction(self, txn: Dict[str, Any]) -> bool:
        """
        Save transaction to PostgreSQL
        Returns: True if successful
        """
        try:
            self.db.insert_transaction(txn)
            return True
        except Exception as e:
            logger.error(f"Failed to save transaction {txn.get('transaction_id')}: {e}")
            return False
    
    def emit_enriched_transaction(self, txn: Dict[str, Any]) -> bool:
        """
        Emit enriched transaction to Kafka
        Returns: True if successful
        """
        try:
            message = json.dumps(txn).encode('utf-8')
            key = txn['user_id'].encode('utf-8')
            
            self.producer.produce(
                topic='features',
                key=key,
                value=message,
                callback=self._delivery_report
            )
            self.producer.flush()
            return True
        except Exception as e:
            logger.error(f"Failed to emit transaction {txn.get('transaction_id')}: {e}")
            return False
    
    def _delivery_report(self, err, msg):
        """Kafka delivery callback"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
    
    def process_message(self, msg_value: str) -> bool:
        try:
            # Deserialize
            txn = json.loads(msg_value)
            
            # Validate
            is_valid, error = self.validate_transaction(txn)
            if not is_valid:
                logger.warning(f"Validation failed for {txn.get('transaction_id')}: {error}")
                return False
            
            # Enrich
            txn = self.enrich_transaction(txn)
            
            # Save
            if not self.save_transaction(txn):
                return False
            
            # Emit
            if not self.emit_enriched_transaction(txn):
                return False
            
            self.processed += 1
            if self.processed % 1000 == 0:
                logger.info(f"Processed {self.processed} transactions")
            
            return True
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}", exc_info=True)
            return False
    
    def run(self):
        """Main consumer loop"""
        logger.info("Starting transaction processing loop...")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Process message
                success = self.process_message(msg.value().decode('utf-8'))
                
                # Only commit if successful
                if success:
                    try:
                        self.consumer.commit(asynchronous=False)
                    except Exception as e:
                        logger.error(f"Failed to commit offset: {e}")
                else:
                    self.errors += 1
        
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Fatal error in processing loop: {e}", exc_info=True)
            raise
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("Cleaning up resources...")
        self.consumer.close()
        self.producer.flush()
        self.db.close()
        logger.info(f"Stats - Processed: {self.processed}, Errors: {self.errors}")
