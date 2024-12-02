import time
import logging
from datetime import datetime
from botocore.exceptions import ClientError

class S3LogHandler(logging.Handler):
    """Custom logging handler that uploads logs to S3"""
    
    def __init__(self, s3_client, bucket: str, prefix: str, upload_interval: int = 300):
        """
        Initialise S3 log handler
        
        Args:
            bucket: S3 bucket name
            prefix: Prefix for S3 keys (folder structure)
            upload_interval: How often to upload logs (in seconds)
        """
        super().__init__()
        self.s3_client = s3_client
        self.bucket = bucket
        self.prefix = prefix
        self.upload_interval = upload_interval
        self.buffer = []
        self.last_upload = time.time()

    def emit(self, record):
        """Process a log record"""
        msg = self.format(record)
        self.buffer.append(msg)
        
        # Upload if enough time has passed
        if time.time() - self.last_upload > self.upload_interval:
            self.upload_logs()

    def upload_logs(self):
        """Upload accumulated logs to S3"""
        if not self.buffer:
            return
            
        # Create log content
        log_content = '\n'.join(self.buffer)
        
        # Generate S3 key with timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        s3_key = f"{self.prefix}/monzo_etl_{timestamp}.log"
        
        try:
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=log_content
            )
            
            # Clear buffer and update upload time
            self.buffer = []
            self.last_upload = time.time()
            
        except ClientError as e:
            # Keep logs in buffer if upload fails
            print(f"Failed to upload logs to S3: {str(e)}")