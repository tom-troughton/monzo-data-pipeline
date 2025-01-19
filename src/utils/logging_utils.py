import logging
import boto3
from logging.handlers import RotatingFileHandler
from datetime import datetime

class Logger:
    def __init__(self, log_file_path: str, s3_bucket: str, s3_prefix: str, logger_name: str, run_id: str):
        self.log_file_path = f"{log_file_path}_{run_id}.log"
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client('s3')
        self.logger = self._setup_logger(logger_name)

    def _setup_logger(self, logger_name):
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        # File handler
        file_handler = RotatingFileHandler(self.log_file_path, maxBytes=5*1024*1024, backupCount=2)
        file_handler.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Creating a formatter and set it for both handlers
        formatter = logging.Formatter('%(asctime)s - monzo-etl - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def upload_log_to_s3(self):
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            s3_key = f"{self.s3_prefix}/monzo_etl_{timestamp}.log"
            self.s3_client.upload_file(self.log_file_path, self.s3_bucket, s3_key)
            self.logger.info(f"Successfully uploaded log file to S3: {s3_key}")
        except Exception as e:
            self.logger.error(f"Failed to upload log file to S3: {str(e)}")