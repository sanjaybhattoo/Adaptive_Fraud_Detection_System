import pytest
import os
from unittest.mock import Mock, patch


@pytest.fixture
def kafka_brokers():
    """Kafka brokers fixture"""
    return "localhost:9092"


@pytest.fixture
def pg_url():
    """PostgreSQL URL fixture"""
    return "postgresql://test_user:test_pass@localhost:5432/test_db"


@pytest.fixture
def mongo_url():
    """MongoDB URL fixture"""
    return "mongodb://test_user:test_pass@localhost:27017/test_db"


@pytest.fixture
def sample_transaction():
    """Sample transaction fixture"""
    return {
        'transaction_id': 'txn_12345',
        'user_id': 'user_123',
        'card_id': 'card_456',
        'merchant_id': 'merchant_789',
        'amount': 150.00,
        'timestamp': '2024-03-07T14:23:45Z',
        'ip_address': '203.0.113.45',
        'device_id': 'dev_111',
        'user_location': {'lat': 40.7128, 'lng': -74.0060},
        'billing_address': '123 Main St, NYC',
        'shipping_address': '456 Oak Ave, LA',
    }


@pytest.fixture
def mock_processor():
    """Mock processor fixture"""
    with patch('src.processor.TransactionProcessor'):
        from src.processor import TransactionProcessor
        processor = TransactionProcessor.__new__(TransactionProcessor)
        processor.consumer = Mock()
        processor.producer = Mock()
        processor.db = Mock()
        processor.processed = 0
        processor.errors = 0
        return processor


@pytest.fixture
def env_vars(monkeypatch):
    """Set environment variables for tests"""
    monkeypatch.setenv('KAFKA_BROKERS', 'localhost:9092')
    monkeypatch.setenv('POSTGRES_URL', 'postgresql://user:pass@localhost:5432/db')
    monkeypatch.setenv('MONGO_URL', 'mongodb://user:pass@localhost:27017/db')
    monkeypatch.setenv('LOG_LEVEL', 'INFO')
