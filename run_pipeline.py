#!/usr/bin/env python3
"""
ETL Pipeline Entry Point Script
Configures logging and runs the complete pipeline.
"""
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent))

from src.config import Config
from src.pipeline import run_pipeline


def setup_logging():
    """Configure logging with both file and console handlers."""
    # Create logs directory if it doesn't exist
    Config.LOG_DIR.mkdir(exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = Config.LOG_DIR / f'pipeline_{timestamp}.log'
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    
    return logger


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Run ETL pipeline to fetch data from APIs and load into PostgreSQL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py
  python run_pipeline.py --batch-size 10
  python run_pipeline.py --log-level DEBUG
        """
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1,
        help='Number of records to fetch per API (default: 1)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default=Config.LOG_LEVEL,
        help=f'Set logging level (default: {Config.LOG_LEVEL})'
    )
    
    return parser.parse_args()


def main():
    """Main entry point for the ETL pipeline."""
    args = parse_arguments()
    
    # Override log level if specified
    if args.log_level != Config.LOG_LEVEL:
        Config.LOG_LEVEL = args.log_level
    
    # Setup logging
    logger = setup_logging()
    
    try:
        # Validate configuration
        logger.info("Validating configuration...")
        Config.validate()
        logger.info("Configuration validated successfully")
        
        # Run pipeline
        logger.info(f"Starting pipeline with batch_size={args.batch_size}")
        success = run_pipeline(batch_size=args.batch_size, triggered_by="manual")
        
        if success:
            logger.info("=" * 50)
            logger.info("PIPELINE EXECUTION SUCCESSFUL")
            logger.info("=" * 50)
            return 0
        else:
            logger.error("=" * 50)
            logger.error("PIPELINE EXECUTION FAILED")
            logger.error("=" * 50)
            return 1
    
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return 130
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())