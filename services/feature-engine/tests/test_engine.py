import pytest
import json
from unittest.mock import Mock, patch


class TestFeatureEngine:
    @patch('src.engine.Consumer')
    @patch('src.engine.Producer')
    @patch('src.engine.RedisCache')
    @patch('src.engine.Database')
    def test_engine_initialization(self, mock_db, mock_cache, mock_producer, mock_consumer):
        from src.engine import FeatureEngine
        engine = FeatureEngine(
            kafka_brokers='localhost:9092',
            pg_url='postgresql://test:test@localhost:5432/test',
            redis_url='redis://localhost:6379'
        )
        assert engine is not None
        assert engine.processed == 0
        assert engine.errors == 0
