import logging
import os
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor
import boto3
from botocore.exceptions import ClientError
import asyncio

logger = logging.getLogger(__name__)

class S3Uploader:
    """Handles uploading Parquet datasets to S3 with retries and parallel uploads."""
    
    def __init__(self, max_workers: int = 4, max_retries: int = 3):
        self.s3_client = boto3.client('s3')
        self.max_workers = max_workers
        self.max_retries = max_retries

    def upload_to_s3(
        self,
        local_path: str,
        s3_bucket: str,
        s3_key_prefix: str,
        delete_local: bool = False
    ) -> Tuple[bool, List[str]]:
        """
        Upload a local directory or file to S3.
        
        Args:
            local_path: Path to local file or directory
            s3_bucket: Target S3 bucket name
            s3_key_prefix: Prefix for S3 keys (folder structure)
            delete_local: Whether to delete local files after successful upload
        
        Returns:
            Tuple of (success: bool, uploaded_files: List[str])
        """
        if not os.path.exists(local_path):
            logger.error(f"Local path does not exist: {local_path}")
            return False, []

        try:
            uploaded_files = []
            failed_files = []

            if os.path.isfile(local_path):
                # Single file upload - s3_key_prefix is treated as a directory
                success = self._upload_file_with_retry(local_path, s3_bucket, s3_key_prefix)
                if success:
                    uploaded_files.append(local_path)
                else:
                    failed_files.append(local_path)
            else:
                # Directory upload with parallel processing
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = []
                    for root, _, files in os.walk(local_path):
                        for file in files:
                            if not file.endswith('.parquet'):
                                continue
                            
                            local_file = os.path.join(root, file)
                            # Preserve directory structure in S3
                            rel_path = os.path.relpath(local_file, local_path)
                            s3_key = os.path.join(s3_key_prefix, rel_path).replace('\\', '/')
                            
                            future = executor.submit(
                                self._upload_file_with_retry,
                                local_file, s3_bucket, s3_key
                            )
                            futures.append((future, local_file))

                    # Collect results
                    for future, local_file in futures:
                        try:
                            if future.result():
                                uploaded_files.append(local_file)
                            else:
                                failed_files.append(local_file)
                        except Exception as e:
                            logger.error(f"Upload failed for {local_file}: {e}")
                            failed_files.append(local_file)

            if failed_files:
                logger.error(f"Failed to upload {len(failed_files)} files: {failed_files}")
                return False, uploaded_files

            if delete_local and uploaded_files:
                self._cleanup_local_files(uploaded_files, local_path)

            logger.info(f"Successfully uploaded {len(uploaded_files)} files to s3://{s3_bucket}/{s3_key_prefix}")
            return True, uploaded_files

        except Exception as e:
            logger.exception(f"Error during S3 upload operation: {e}")
            return False, uploaded_files

    def _upload_file_with_retry(self, local_file: str, bucket: str, s3_key: str) -> bool:
        """Attempt to upload a file with retries."""
        for attempt in range(self.max_retries):
            try:
                if self._upload_file(local_file, bucket, s3_key):
                    return True
            except (ClientError, Exception) as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"Final retry failed for {local_file}: {e}")
                    return False
                logger.warning(f"Retry {attempt + 1}/{self.max_retries} for {local_file}: {e}")
                # Add exponential backoff
                asyncio.sleep(2 ** attempt)
        return False

    def _upload_file(self, local_file: str, bucket: str, s3_key: str) -> bool:
        """Upload a single file to S3. Raises exceptions on failure for retry logic."""
        try:
            # Ensure s3_key is treated as a prefix for single files
            if not s3_key.endswith(os.path.basename(local_file)):
                s3_key = os.path.join(s3_key, os.path.basename(local_file)).replace('\\', '/')
            
            self.s3_client.upload_file(local_file, bucket, s3_key)
            logger.debug(f"Uploaded {local_file} to s3://{bucket}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_file}: {e}")
            raise  # Re-raise for retry logic

    def _cleanup_local_files(self, uploaded_files: List[str], base_path: str) -> None:
        """Clean up local files and empty directories after successful upload."""
        try:
            for file_path in uploaded_files:
                try:
                    os.remove(file_path)
                except OSError as e:
                    logger.warning(f"Failed to delete {file_path}: {e}")

            # Clean up empty directories
            if os.path.isdir(base_path):
                for root, dirs, files in os.walk(base_path, topdown=False):
                    for dir_name in dirs:
                        dir_path = os.path.join(root, dir_name)
                        try:
                            if not os.listdir(dir_path):
                                os.rmdir(dir_path)
                        except OSError:
                            pass
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
