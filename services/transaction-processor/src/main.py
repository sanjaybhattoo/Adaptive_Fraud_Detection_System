"""
Transaction Processor Service
Main entry point
"""

import json
import logging
import os
import sys
from typing import Optional

from confluent_kafka import Consumer, KafkaError

from processor import TransactionProcessor
from utils import setup_logging, wait_for_dependencies

# Setup logging
logger = logging.getLogger(__name__)
setup_logging(os.getenv('LOG_LEVEL', 'INFO'))


def create_processor() -> Optional[TransactionProcessor]:
    """Initialize transaction processor"""
    try:
        logger.info("Initializing Transaction Processor...")
        processor = TransactionProcessor()
        logger.info(" Transaction Processor initialized successfully")
        return processor
    except Exception as e:
        logger.error(f" Failed to initialize processor: {e}", exc_info=True)
        return None


def main():
    """Main entry point"""
    logger.info("=" * 80)
    logger.info("Transaction Processor Service Starting")
    logger.info("=" * 80)
    
    # Wait for dependencies (Kafka, PostgreSQL, MongoDB)
    if not wait_for_dependencies():
        logger.error("Dependencies not ready. Exiting.")
        sys.exit(1)
    
    # Initialize processor
    processor = create_processor()
    if not processor:
        logger.error("Failed to create processor. Exiting.")
        sys.exit(1)
    
    # Run
    try:
        logger.info(" Starting message processing loop...")
        processor.run()
    except KeyboardInterrupt:
        logger.info("\n Shutdown signal received")
    except Exception as e:
        logger.error(f" Unexpected error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Cleaning up resources...")
        processor.cleanup()
        logger.info("Shutdown complete")


if __name__ == '__main__':
    main()
