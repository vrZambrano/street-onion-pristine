# CLAUDE.md

## Project Overview
Python script for downloading IBOV (Índice Bovespa) data from B3 (Brasil, Bolsa, Balcão) website using two different methods: Selenium WebDriver and HTTP requests. Downloaded files are automatically uploaded to AWS S3.

## Dependencies
- Python 3.x
- requests>=2.25.1
- selenium>=4.0.0
- boto3>=1.26.0
- python-dotenv>=0.19.0
- pandas>=1.3.0
- pyarrow>=5.0.0
- Chrome browser (for Selenium method)
- ChromeDriver (for Selenium method)

## Installation
```bash
pip install -r requirements.txt
```

## Environment Variables
Create a `.env` file in the project root with:
```
AWS_ACCESS_KEY=your_access_key
AWS_SECRET=your_secret_key
AWS_REGION=your_region
AWS_BUCKET=your_bucket_name
```

## Usage
Run the main script:
```bash
python main.py
```

The script will:
1. First try to download using Selenium (more reliable)
2. If Selenium fails, fallback to HTTP requests method
3. Save downloaded CSV files to `./data/` directory
4. Automatically convert CSV files to Parquet format
5. Upload Parquet files to configured S3 bucket with partitioning by date (ano/mes/dia)

## Project Structure
- `main.py` - Main application with B3DataDownloader class
- `requirements.txt` - Python dependencies
- `data/` - Directory for downloaded files (auto-created)
- `.env` - Environment variables for AWS configuration

## Testing
No specific test framework configured. Check the README or search codebase for testing approach.

## Notes
- The script handles both CSV and ZIP file downloads
- Downloads are saved with timestamp in filename
- Files are automatically converted from CSV to Parquet format for better compression and query performance
- Parquet files are uploaded to S3 with path: `ibov_data/ano=YYYY/mes=MM/dia=DD/filename.parquet` for efficient partitioning
- Includes error handling and multiple download strategies
- Headless Chrome browser is used for web scraping
- S3 upload happens automatically after successful download
- Parquet format provides better storage efficiency and faster query performance compared to CSV
