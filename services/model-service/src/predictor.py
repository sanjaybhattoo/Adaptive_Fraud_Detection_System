import json
import logging
from typing import Dict, Any

from confluent_kafka import Consumer, Producer, KafkaError

from model import FraudDetectionModel
from preprocessing import FeaturePreprocessor
from utils import convert_to_risk_score

logger = logging.getLogger(__name__)


class FraudPredictor:
    def __init__(self,
                 kafka_brokers: str = None,
                 model_path: str = None,
                 scaler_path: str = None):
        import os
        
        self.kafka_brokers = kafka_brokers or os.getenv('KAFKA_BROKERS', 'localhost:9092')
        self.model_path = model_path or os.getenv('MODEL_PATH')
        self.scaler_path = scaler_path or os.getenv('SCALER_PATH')
        self.consumer_group = os.getenv('CONSUMER_GROUP', 'model-service-group')
        self.input_topic = os.getenv('INPUT_TOPIC', 'features-computed')
        self.output_topic = os.getenv('OUTPUT_TOPIC', 'risk-scores')
        self.model = FraudDetectionModel(self.model_path, self.scaler_path)
        self.preprocessor = FeaturePreprocessor()
        self.consumer = self._init_consumer()
        self.producer = self._init_producer()
        self.processed = 0
        self.errors = 0
        
        logger.info(f"Predictor initialized - Kafka: {self.kafka_brokers}")
    
    def _init_consumer(self) -> Consumer:
        brokers = self.kafka_brokers.split(',')
        
        config = {
            'bootstrap.servers': ','.join(brokers),
            'group.id': self.consumer_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        
        consumer = Consumer(config)
        consumer.subscribe([self.input_topic])
        logger.info(f"Consumer created - topic: {self.input_topic}")
        
        return consumer
    
    def _init_producer(self) -> Producer:
        """Initialize Kafka producer"""
        brokers = self.kafka_brokers.split(',')
        
        config = {
            'bootstrap.servers': ','.join(brokers),
            'acks': 'all',
            'retries': 3,
        }
        
        logger.info("Producer created")
        return Producer(config)
    
    def predict_for_transaction(self, txn: Dict[str, Any]) -> Dict[str, Any]:
        try:
            feature_vector = self.preprocessor.extract_features(txn)
            fraud_prob, importances = self.model.predict(feature_vector)
            risk_score = convert_to_risk_score(fraud_prob)
            top_features = self.preprocessor.get_top_features(importances, top_n=5)
            
            return {
                'risk_score': risk_score,
                'fraud_probability': fraud_prob,
                'top_features': top_features,
            }
        
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return {
                'risk_score': 50.0,
                'fraud_probability': 0.5,
                'top_features': [],
            }
    
    def emit_risk_score(self, txn: Dict[str, Any], prediction: Dict[str, Any]) -> bool:
        try:
            output = {
                'transaction_id': txn['transaction_id'],
                'user_id': txn['user_id'],
                'risk_score': prediction['risk_score'],
                'fraud_probability': prediction['fraud_probability'],
                'model_version': 'v3.2.1',
                'timestamp': txn.get('timestamp'),
                'top_risk_factors': prediction['top_features'],
            }
            
            message = json.dumps(output).encode('utf-8')
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
            logger.error(f"Failed to emit risk score: {e}")
            return False
    
    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
    
    def process_message(self, msg_value: str) -> bool:
        try:
            txn = json.loads(msg_value)
            prediction = self.predict_for_transaction(txn)
            if not self.emit_risk_score(txn, prediction):
                return False
            
            self.processed += 1
            if self.processed % 1000 == 0:
                logger.info(f"Processed {self.processed} predictions")
            
            return True
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to deserialize: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            return False
    
    def run(self):
        logger.info("Starting prediction loop...")
        
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
                        logger.error(f"Failed to commit: {e}")
                else:
                    self.errors += 1
        
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
            raise
    
    def cleanup(self):
        logger.info("Cleaning up...")
        self.consumer.close()
        self.producer.flush()
        logger.info(f"Stats - Processed: {self.processed}, Errors: {self.errors}")
