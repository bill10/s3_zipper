import os
import boto3
import zipfile
import tempfile
import logging
import yaml
import argparse
from pathlib import Path
from dotenv import load_dotenv
from botocore.exceptions import ClientError

class S3FolderZipper:
    def __init__(self, config_path, dry_run=False):
        """
        Initialize S3 Folder Zipper from YAML config
        
        :param config_path: Path to YAML configuration file
        :param dry_run: If True, simulate the process without downloading/uploading files
        """
        # Load environment variables from .env file
        self._load_environment()
        
        self.config = self._load_config(config_path)
        self.s3_client = self._initialize_s3_client()
        self.dry_run = dry_run
        
        # Set up logging
        self._setup_logging()
    
    def _load_environment(self):
        """Load environment variables from .env file"""
        env_path = Path('.env')
        if not env_path.exists():
            raise ValueError(
                "'.env' file not found. Please create one based on '.env.example'"
            )
        
        load_dotenv()
        
        # Verify required AWS credentials are present
        required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            raise ValueError(
                f"Missing required AWS credentials in .env file: {', '.join(missing_vars)}"
            )
    
    def _initialize_s3_client(self):
        """Initialize S3 client with credentials from environment"""
        # Get credentials from environment variables
        credentials = {
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        }
        
        # Add region if specified in config or environment
        region = (
            self.config['aws'].get('region') or 
            os.getenv('AWS_DEFAULT_REGION')
        )
        if region:
            credentials['region_name'] = region
        
        return boto3.client('s3', **credentials)
        
    def _load_config(self, config_path):
        """Load and validate YAML configuration"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Validate required fields
        required_fields = {
            'aws': ['source_bucket', 'destination_bucket'],
            'zip_config': ['source_prefixes', 'output_zip_name'],
        }

        for section, fields in required_fields.items():
            if section not in config:
                raise ValueError(f"Missing required section: {section}")
            for field in fields:
                if field not in config[section]:
                    raise ValueError(f"Missing required field: {section}.{field}")

        # Ensure source prefixes end with '/'
        for i, prefix in enumerate(config['zip_config']['source_prefixes']):
            if not prefix.endswith('/'):
                config['zip_config']['source_prefixes'][i] = prefix + '/'

        return config

    def _setup_logging(self):
        """Configure logging based on YAML config"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))
        log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
        
        # Create log directory if file logging is enabled
        if 'file' in log_config:
            os.makedirs(os.path.dirname(log_config['file']), exist_ok=True)
        
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.StreamHandler(),
                *([] if 'file' not in log_config else [
                    logging.FileHandler(log_config['file'])
                ])
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _list_s3_files(self, prefix):
        """List files in an S3 prefix"""
        files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.config['aws']['source_bucket'], Prefix=prefix)
        
        for page in pages:
            if 'Contents' not in page:
                self.logger.warning(f"No contents found for prefix: {prefix}")
                continue
            
            for obj in page['Contents']:
                # Skip if the object is the folder itself
                if obj['Key'] == prefix:
                    continue
                    
                files.append(obj['Key'])
        
        return files

    def _download_files(self, files, local_dir):
        """Download files from S3"""
        downloaded_files = []
        skipped_files = []
        
        for file in files:
            # Extract the lowest level folder name and file name
            path_parts = Path(file).parts
            if len(path_parts) > 1:
                # Use only the last folder name and file name for the local path
                local_path = os.path.join(path_parts[-2], path_parts[-1])
            else:
                local_path = path_parts[-1]
            
            # Construct full local file path
            local_file_path = os.path.join(local_dir, local_path)
            
            # Check if file already exists and has content
            if os.path.exists(local_file_path) and os.path.getsize(local_file_path) > 0:
                self.logger.info(f"File already exists locally, skipping download: {local_file_path}")
                skipped_files.append(local_file_path)
                continue
            
            # Ensure local directory exists
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            
            # Download the file
            try:
                self.s3_client.download_file(
                    self.config['aws']['source_bucket'], 
                    file, 
                    local_file_path
                )
                downloaded_files.append(local_file_path)
                self.logger.info(f"Downloaded: {file} to {local_file_path}")
            except Exception as e:
                self.logger.error(f"Error downloading {file}: {e}")
                if os.path.exists(local_file_path):
                    os.remove(local_file_path)  # Remove partially downloaded file
                raise
        
        return downloaded_files + skipped_files

    def _create_zip(self, files_dir):
        """Create a zip file from downloaded files"""
        # Get the output directory (same as files_dir in this case)
        output_dir = files_dir
        
        # Create zip file in the output directory
        zip_name = self.config['zip_config']['output_zip_name']
        zip_path = os.path.join(output_dir, zip_name)
        
        # Check if zip file already exists and is valid
        if os.path.exists(zip_path):
            try:
                with zipfile.ZipFile(zip_path, 'r') as zipf:
                    # Test the zip file integrity
                    if zipf.testzip() is None:
                        self.logger.info(f"Valid zip file already exists: {zip_path}")
                        return zip_path
            except zipfile.BadZipFile:
                self.logger.warning(f"Existing zip file is corrupt, recreating: {zip_path}")
                os.remove(zip_path)
        
        compression = zipfile.ZIP_DEFLATED
        compression_level = self.config['options'].get('compression_level', 9)
        
        self.logger.info(f"Creating zip file: {zip_path}")
        with zipfile.ZipFile(
            zip_path, 'w',
            compression=compression,
            compresslevel=compression_level
        ) as zipf:
            for root, _, files in os.walk(files_dir):
                for file in files:
                    # Skip if this is the zip file itself
                    if file == zip_name:
                        continue
                        
                    file_path = os.path.join(root, file)
                    
                    # Get the relative path from the files directory
                    rel_path = os.path.relpath(file_path, files_dir)
                    
                    # Extract the lowest level folder name from the path
                    path_parts = Path(rel_path).parts
                    if len(path_parts) > 1:
                        # Use only the last folder name and the file name
                        arcname = os.path.join(path_parts[-2], path_parts[-1])
                    else:
                        # If there's no folder, just use the file name
                        arcname = path_parts[-1]
                    
                    zipf.write(file_path, arcname=arcname)
                    self.logger.debug(f"Added to zip: {arcname}")
        
        self.logger.info(f"Created zip file: {zip_path}")
        return zip_path

    def _upload_zip(self, zip_path):
        """Upload zip file to S3"""
        dest_prefix = self.config['zip_config'].get('destination_prefix', '')
        dest_bucket = self.config['aws']['destination_bucket']
        dest_key = os.path.join(dest_prefix, os.path.basename(zip_path)).replace('\\', '/')
        
        # Check if file already exists in S3
        try:
            self.s3_client.head_object(
                Bucket=dest_bucket,
                Key=dest_key
            )
            if not self.config['options'].get('overwrite_s3', False):
                self.logger.info(f"File exists in S3 and overwrite_s3 is False, skipping upload: s3://{dest_bucket}/{dest_key}")
                return
            else:
                self.logger.info(f"File exists in S3 and overwrite_s3 is True, uploading: s3://{dest_bucket}/{dest_key}")
        except ClientError:
            self.logger.info(f"File does not exist in S3, uploading: s3://{dest_bucket}/{dest_key}")
        
        # Upload the file
        try:
            self.logger.info(f"Uploading zip to: {dest_key}")
            self.s3_client.upload_file(
                zip_path,
                dest_bucket,
                dest_key
            )
            self.logger.info(f"Successfully uploaded zip to: {dest_key}")
        except Exception as e:
            self.logger.error(f"Error uploading zip: {e}")
            raise

    def process_folders(self):
        """Main process to download folders, create zip, and upload"""
        try:
            # Get all files from source prefixes
            all_files = []
            for prefix in self.config['zip_config']['source_prefixes']:
                files = self._list_s3_files(prefix)
                if not files:
                    logging.warning(f"No files found for prefix: {prefix}")
                all_files.extend(files)

            if not all_files:
                logging.error("No files found in any of the specified prefixes")
                return

            logging.info(f"Found {len(all_files)} files to process")
            
            # In dry run mode, just list what would be processed
            if self.dry_run:
                self._simulate_process(all_files)
                return

            # Get or create local directory
            local_dir = self.config['zip_config'].get('local_directory', 'output')
            local_dir = os.path.abspath(local_dir)
            os.makedirs(local_dir, exist_ok=True)
            
            # Download files (will skip existing ones)
            downloaded_files = self._download_files(all_files, local_dir)
            
            # Create zip file (will use existing if valid)
            zip_path = self._create_zip(local_dir)
            
            # Upload zip file (will check if exists in S3)
            self._upload_zip(zip_path)
            
            # Clean up if configured
            if self.config['options'].get('delete_local_after', True):
                logging.info("Cleaning up local files")
                for file in downloaded_files:
                    if os.path.exists(file):
                        os.remove(file)
                if os.path.exists(zip_path):
                    os.remove(zip_path)
                # Remove empty directories
                for root, dirs, files in os.walk(local_dir, topdown=False):
                    for name in dirs:
                        try:
                            os.rmdir(os.path.join(root, name))
                        except OSError:
                            pass  # Directory not empty
                try:
                    os.rmdir(local_dir)
                except OSError:
                    pass  # Directory not empty or doesn't exist

        except Exception as e:
            logging.error(f"Error processing folders: {str(e)}")
            raise

    def _simulate_process(self, files):
        """Simulate the process in dry run mode"""
        logging.info("=== DRY RUN MODE - No files will be downloaded or uploaded ===")
        
        # Show files that would be downloaded
        logging.info("\nFiles that would be downloaded:")
        for file in files:
            logging.info(f"  - s3://{self.config['aws']['source_bucket']}/{file}")
        
        # Show zip file that would be created
        zip_name = self.config['zip_config']['output_zip_name']
        logging.info(f"\nZip file that would be created: {zip_name}")
        
        # Show destination
        dest_prefix = self.config['zip_config'].get('destination_prefix', '')
        dest_bucket = self.config['aws']['destination_bucket']
        dest_path = f"s3://{dest_bucket}/{dest_prefix}{zip_name}"
        logging.info(f"\nZip file would be uploaded to: {dest_path}")
        
        # Show compression info
        compression = self.config['options'].get('compression_level', 9)
        logging.info(f"\nCompression level that would be used: {compression}")
        
        logging.info("\n=== End of Dry Run Summary ===")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Download multiple S3 folders, combine into a zip, and upload back to S3'
    )
    parser.add_argument(
        '-c', '--config',
        type=str,
        help='Path to YAML configuration file',
        default='config.yaml'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simulate the process without downloading or uploading files'
    )
    args = parser.parse_args()

    # Convert to absolute path if relative path is provided
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = Path.cwd() / config_path

    if not config_path.exists():
        print(f"Error: Configuration file not found: {config_path}")
        return 1

    try:
        # Initialize zipper with config and dry run flag
        zipper = S3FolderZipper(config_path, dry_run=args.dry_run)
        zipper.process_folders()
        return 0
    except ValueError as e:
        print(f"Configuration Error: {str(e)}")
        return 1
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == '__main__':
    exit(main())