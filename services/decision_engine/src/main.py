import logging
import os
import sys

from engine import DecisionEngine
from utils import setup_logging, wait_for_dependencies

logger = logging.getLogger(__name__)
setup_logging(os.getenv('LOG_LEVEL', 'INFO'))

def main():
    """Main entry point"""
    logger.info("=" * 80)
    logger.info(" Service Starting")
    logger.info("=" * 80)
    
    if not wait_for_dependencies():
        logger.error(" not ready. Exiting.")
        sys.exit(1)
    
    engine = create_engine()
    if not engine:
        logger.error("Failed to create engine. Exiting.")
        sys.exit(1)
    
    try:
        logger.info(" decision making loop...")
        engine.run()
    except KeyboardInterrupt:
        logger.info("\n signal received")
    except Exception as e:
        logger.error(f" error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Cleaning up resources...")
        engine.cleanup()
        logger.info(" complete")


if __name__ == '__main__':
    main()
