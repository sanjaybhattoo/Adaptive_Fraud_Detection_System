import json
import logging
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, Producer, KafkaError

from cache import RedisCache
from db import Database
from features import FeatureComputer
from utils import parse_timestamp

logger = logging.getLogger(__name__)
class FeatureEngine:
    def __init__(self, 
                 kafka_brokers: str = None, 
                 pg_url: str = None,
                 redis_url: str = None):
        import os
        
        self.kafka_brokers = kafka_brokers or os.getenv('KAFKA_BROKERS', 'localhost:9092')
        self.pg_url = pg_url or os.getenv('POSTGRES_URL')
        self.redis_url = redis_url or os.getenv('REDIS_URL')
        self.consumer_group = os.getenv('CONSUMER_GROUP', 'feature-engine-group')
        self.input_topic = os.getenv('INPUT_TOPIC', 'features')
        self.output_topic = os.getenv('OUTPUT_TOPIC', 'features-computed')
        self.consumer = self._init_consumer()
        self.producer = self._init_producer()
        self.cache = RedisCache(self.redis_url)
        self.db = Database(self.pg_url)
        self.computer = FeatureComputer(self.cache, self.db)
        self.processed = 0
        self.errors = 0
        
        logger.info(f"Engine initialized - Kafka brokers: {self.kafka_brokers}")
    
    def _init_consumer(self) -> Consumer:
        brokers = self.kafka_brokers.split(',')
        
        config = {
            'bootstrap.servers': ','.join(brokers),'group.id': self.consumer_group,
            'auto.offset.reset': 'earliest','enable.auto.commit': False,
            'session.timeout.ms': 30000,
        }
        
        consumer = Consumer(config)
        consumer.subscribe([self.input_topic])
        logger.info(f" consumer create: {self.input_topic}")
        
        return consumer
    
    def _init_producer(self) -> Producer:
        brokers = self.kafka_brokers.split(',')
        
        config = {
            'bootstrap.servers': ','.join(brokers),'acks': 'all',
            'retries': 3,
        }
        logger.info("=producer created")
        return Producer(config)
    
    def compute_features(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        user_id = txn['user_id']
        
        try:
            velocity = self.computer.compute_velocity_features(user_id, txn)
            behavioral = self.computer.compute_behavioral_features(user_id, txn)
            historical = self.computer.compute_historical_features(user_id)
            geographic = self.computer.compute_geographic_features(txn)
            txn['computed_features'] = {
                **velocity,**historical,**behavioral,
                **geographic,
                'user_profile': txn.get('user_profile'),
                'merchant_profile': txn.get('merchant_profile'),
            }
            
            return txn
        
        except Exception as e:
            logger.error(f"Failed to compute features: {e}")
            raise
    
    def emit_features(self, txn: Dict[str, Any]) -> bool:
        try:
            message = json.dumps(txn).encode('utf-8')
            key = txn['user_id'].encode('utf-8')
            
            self.producer.produce(
                topic=self.output_topic,
                key=key,
                value=message,
                callback=self._delivery_report
            )
            self.producer.flush()
            return True
        except Exception as e:
            logger.error(f"Failed to emit features: {e}")
            return False
    
    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f" delivery failed: {err}")
        else:
            logger.debug(f"Message to {msg.topic()}")
    
    def process_message(self, msg_value: str) -> bool:
        try:
            txn = json.loads(msg_value)
            txn = self.compute_features(txn)
            if not self.emit_features(txn):
                return False
            
            self.processed += 1
            if self.processed % 1000 == 0:
                logger.info(f"{self.processed} transactions")
            
            return True
        
        except json.JSONDecodeError as e:
            logger.error(f" deserialize message: {e}")
            return False
        except Exception as e:
            logger.error(f"error processing message: {e}", exc_info=True)
            return False
    
    def run(self):
        logger.info("Starting feature computation loop...")
        
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
                success = self.process_message(msg.value().decode('utf-8'))
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
        logger.info("Cleaning up resources...")
        self.consumer.close()
        self.producer.flush()
        self.cache.close()
        self.db.close()
        logger.info(f"Stats - Processed: {self.processed}, Errors: {self.errors}")
