# S3 Folder Zipper

## Overview
This Python script downloads multiple folders from an AWS S3 bucket, combines them into a single zip file, and uploads the zip file back to S3.

## Features
- Download multiple S3 folders by prefix
- Preserve folder structure in zip file
- Upload combined zip file to S3
- YAML-based configuration
- Environment-based AWS credentials
- Comprehensive logging
- Error handling
- Skip existing files option
- Custom compression levels

## Prerequisites
- Python 3.7+
- AWS Credentials (configured via .env file)

## Installation
1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Create `.env` file from template:
```bash
cp .env.example .env
# Edit .env with your AWS credentials
```

## AWS Credentials
Create a `.env` file with your AWS credentials:
```bash
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=your_region_here  # Optional, can be set in config.yaml
```

## Usage
```bash
# Use default config.yaml in current directory
python s3_folder_zipper.py

# Specify a custom config file
python s3_folder_zipper.py -c /path/to/custom-config.yaml
```

## Configuration
Create a YAML configuration file (e.g., `config.yaml`) with your settings:

```yaml
aws:
  source_bucket: source-bucket-name
  destination_bucket: destination-bucket-name
  region: us-east-1  # Optional

zip_config:
  source_prefixes:  # List of S3 folder prefixes to download
    - "data/folder1/"
    - "data/folder2/subfolder/"
  output_zip_name: output.zip
  destination_prefix: "processed/data/"  # Optional, defaults to root of bucket
  local_directory: "./output"  # Local directory to store zip files

options:
  compression_level: 9
  delete_local_after: true
  overwrite_s3: false  # Set to true to overwrite existing files in S3

logging:
  level: INFO
  format: "%(asctime)s - %(levelname)s - %(message)s"
  file: logs/s3_zipper.log  # Optional
```

### Configuration Details

#### AWS Section
- `source_bucket`: Source S3 bucket name (required)
- `destination_bucket`: Destination S3 bucket name (required)
- `region`: AWS region (optional)

#### Zip Config Section
- `source_prefixes`: List of S3 folder prefixes to download (must end with '/')
- `output_zip_name`: Name of the output zip file
- `destination_prefix`: Prefix for the output zip file in the destination bucket (optional)
- `local_directory`: Local directory for storing zip files

#### Options Section
- `compression_level`: ZIP compression level (1-9, higher = better compression)
- `delete_local_after`: Clean up local files after processing
- `overwrite_s3`: Whether to overwrite existing files in S3

#### Logging Section
- `level`: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- `format`: Log message format
- `file`: Optional log file path

## Command Line Arguments
- `-c, --config`: Path to YAML configuration file (default: `config.yaml` in current directory)

## Security Considerations
- Store AWS credentials in .env file (never commit to version control)
- Use IAM roles with least privilege
- Secure temporary file handling

## Troubleshooting
- Check AWS credentials in .env file
- Verify bucket names and prefixes
- Review logs for detailed error information
- Ensure YAML configuration is valid
- Verify that folder prefixes end with '/'
