#!/usr/bin/env python3
"""
Concall Transcript Downloader
- Scrapes all concall transcripts from Screener.in
- Downloads PDFs
- Uploads to Google Drive organized by FY/Quarter
"""

import os
import re
import json
import base64
import time
import logging
from datetime import datetime
from io import BytesIO

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# =============================================================================
# CONFIGURATION
# =============================================================================

# Google Drive folder ID (Concall Transcripts folder)
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "1ezP5ez-SOuHuU5C13RU-Ndl-g4aa9VVe")

# Timeouts
PAGE_LOAD_TIMEOUT = 30
ELEMENT_WAIT_TIMEOUT = 15
DOWNLOAD_DELAY = 0.5  # Delay between downloads to avoid rate limiting

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# =============================================================================
# GOOGLE DRIVE AUTHENTICATION
# =============================================================================

def get_google_credentials():
    """Get Google credentials from base64 env var or local file."""
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    
    # Try base64 env var first (GitHub Actions)
    creds_base64 = os.getenv("GOOGLE_CREDENTIALS_BASE64")
    if creds_base64:
        creds_json = base64.b64decode(creds_base64).decode("utf-8")
        creds_dict = json.loads(creds_json)
        return Credentials.from_service_account_info(creds_dict, scopes=scopes)
    
    # Fall back to local file
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
    if os.path.exists(creds_file):
        return Credentials.from_service_account_file(creds_file, scopes=scopes)
    
    raise ValueError("No Google credentials found!")

# =============================================================================
# HTTP SESSION
# =============================================================================

def create_session():
    """Create requests session with retry logic."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# =============================================================================
# SELENIUM WEBDRIVER
# =============================================================================

def create_webdriver():
    """Create headless Chrome webdriver."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

# =============================================================================
# SCREENER.IN LOGIN
# =============================================================================

def login_to_screener(driver, username, password):
    """Login to Screener.in"""
    logger.info("Logging in to Screener.in...")
    
    driver.get("https://www.screener.in/login/")
    wait = WebDriverWait(driver, ELEMENT_WAIT_TIMEOUT)
    
    try:
        username_field = wait.until(EC.presence_of_element_located((By.NAME, "username")))
        username_field.clear()
        username_field.send_keys(username)
        
        password_field = driver.find_element(By.NAME, "password")
        password_field.clear()
        password_field.send_keys(password)
        
        login_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        login_button.click()
        
        time.sleep(2)
        
        if "login" not in driver.current_url.lower():
            logger.info("Login successful")
            return True
        else:
            logger.error("Login failed")
            return False
            
    except Exception as e:
        logger.error(f"Login error: {e}")
        return False

# =============================================================================
# SCRAPE TRANSCRIPTS
# =============================================================================

def get_all_companies(driver):
    """Get list of all companies from Screener.in"""
    logger.info("Fetching company list...")
    
    companies = []
    page = 1
    
    while True:
        url = f"https://www.screener.in/screens/71064/all-companies/?page={page}"
        driver.get(url)
        time.sleep(1)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Find company links
        company_links = soup.select("a[href*='/company/']")
        
        if not company_links:
            break
        
        for link in company_links:
            href = link.get("href", "")
            name = link.get_text(strip=True)
            if "/company/" in href and name:
                company_url = f"https://www.screener.in{href}" if href.startswith("/") else href
                companies.append({
                    "name": name,
                    "url": company_url
                })
        
        logger.info(f"Page {page}: Found {len(company_links)} companies")
        
        # Check for next page
        next_button = soup.select_one("a.next, a[rel='next'], .pagination a:contains('Next')")
        if not next_button:
            # Check if we've collected enough or no more pages
            if page > 50:  # Safety limit
                break
            page += 1
        else:
            page += 1
        
        if page > 100:  # Maximum pages to prevent infinite loop
            break
    
    # Remove duplicates
    seen = set()
    unique_companies = []
    for company in companies:
        if company["url"] not in seen:
            seen.add(company["url"])
            unique_companies.append(company)
    
    logger.info(f"Total unique companies: {len(unique_companies)}")
    return unique_companies

def scrape_transcripts_page(driver, session):
    """Scrape transcripts from the main transcripts page."""
    logger.info("Fetching transcripts from main page...")
    
    transcripts = []
    page = 1
    
    while True:
        url = f"https://www.screener.in/transcripts/?page={page}"
        driver.get(url)
        time.sleep(1)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Find transcript entries
        # Screener.in typically shows transcripts in a table or list
        rows = soup.select("table tbody tr, .transcript-item, .document-row")
        
        if not rows:
            # Try alternative selectors
            rows = soup.select("a[href*='transcript'], a[href*='.pdf']")
        
        if not rows:
            logger.info(f"No more transcripts found on page {page}")
            break
        
        for row in rows:
            transcript = parse_transcript_row(row)
            if transcript:
                transcripts.append(transcript)
        
        logger.info(f"Page {page}: Found {len(rows)} items")
        
        # Check for next page
        pagination = soup.select_one("a.next, a[rel='next']")
        if not pagination:
            break
        
        page += 1
        
        if page > 200:  # Safety limit
            break
    
    logger.info(f"Total transcripts found: {len(transcripts)}")
    return transcripts

def parse_transcript_row(row):
    """Parse a transcript row to extract details."""
    try:
        # Try to find PDF link
        pdf_link = None
        links = row.find_all("a", href=True) if hasattr(row, 'find_all') else [row]
        
        for link in links:
            href = link.get("href", "")
            if ".pdf" in href.lower() or "transcript" in href.lower():
                pdf_link = href
                break
        
        if not pdf_link:
            return None
        
        # Make absolute URL
        if pdf_link.startswith("/"):
            pdf_link = f"https://www.screener.in{pdf_link}"
        elif not pdf_link.startswith("http"):
            pdf_link = f"https://www.screener.in/{pdf_link}"
        
        # Extract company name and date
        text = row.get_text(strip=True) if hasattr(row, 'get_text') else str(row)
        
        # Try to extract date (formats: DD-MM-YYYY, DD/MM/YYYY, Month YYYY, etc.)
        date_match = re.search(r'(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})', text)
        date_str = date_match.group(1) if date_match else ""
        
        # Try to extract quarter info
        quarter_match = re.search(r'Q(\d)', text, re.IGNORECASE)
        quarter = f"Q{quarter_match.group(1)}" if quarter_match else ""
        
        # Try to extract fiscal year
        fy_match = re.search(r'FY\s*(\d{2,4})', text, re.IGNORECASE)
        fiscal_year = f"FY{fy_match.group(1)}" if fy_match else ""
        
        # Extract company name (usually first part before date/quarter info)
        company_name = re.split(r'\d|Q\d|FY', text)[0].strip()
        company_name = company_name.replace("Transcript", "").strip()
        
        return {
            "company": company_name[:50],  # Limit length
            "pdf_url": pdf_link,
            "date": date_str,
            "quarter": quarter,
            "fiscal_year": fiscal_year,
            "raw_text": text[:100]
        }
        
    except Exception as e:
        logger.debug(f"Error parsing row: {e}")
        return None

def get_company_transcripts(driver, company_url, session):
    """Get transcripts for a specific company."""
    transcripts = []
    
    try:
        driver.get(company_url)
        time.sleep(0.5)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Look for documents/transcripts section
        # Screener.in typically has a "Documents" section on company pages
        doc_links = soup.select("a[href*='transcript'], a[href*='.pdf']")
        
        for link in doc_links:
            href = link.get("href", "")
            text = link.get_text(strip=True)
            
            # Only include transcripts
            if "transcript" in href.lower() or "transcript" in text.lower():
                if not href.startswith("http"):
                    href = f"https://www.screener.in{href}" if href.startswith("/") else href
                
                # Parse quarter/FY from text
                quarter_match = re.search(r'Q(\d)', text, re.IGNORECASE)
                fy_match = re.search(r'FY\s*(\d{2,4})', text, re.IGNORECASE)
                
                transcripts.append({
                    "pdf_url": href,
                    "quarter": f"Q{quarter_match.group(1)}" if quarter_match else "Unknown",
                    "fiscal_year": f"FY{fy_match.group(1)}" if fy_match else "Unknown",
                    "text": text
                })
        
    except Exception as e:
        logger.debug(f"Error getting transcripts for {company_url}: {e}")
    
    return transcripts

# =============================================================================
# DETERMINE QUARTER AND FISCAL YEAR
# =============================================================================

def determine_quarter_fy(date_str, text=""):
    """Determine quarter and fiscal year from date or text."""
    
    # First try to extract from text
    quarter_match = re.search(r'Q(\d)', text, re.IGNORECASE)
    fy_match = re.search(r'FY\s*(\d{2,4})', text, re.IGNORECASE)
    
    if quarter_match and fy_match:
        quarter = f"Q{quarter_match.group(1)}"
        fy = fy_match.group(1)
        if len(fy) == 2:
            fy = f"20{fy}"
        return quarter, f"FY{fy}"
    
    # Try to parse date
    if date_str:
        try:
            # Try different date formats
            for fmt in ["%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%y", "%d/%m/%y"]:
                try:
                    date = datetime.strptime(date_str, fmt)
                    month = date.month
                    year = date.year
                    
                    # Indian fiscal year: April to March
                    # Q1: Apr-Jun, Q2: Jul-Sep, Q3: Oct-Dec, Q4: Jan-Mar
                    if month >= 4 and month <= 6:
                        quarter = "Q1"
                        fiscal_year = f"FY{year + 1}"
                    elif month >= 7 and month <= 9:
                        quarter = "Q2"
                        fiscal_year = f"FY{year + 1}"
                    elif month >= 10 and month <= 12:
                        quarter = "Q3"
                        fiscal_year = f"FY{year + 1}"
                    else:  # Jan-Mar
                        quarter = "Q4"
                        fiscal_year = f"FY{year}"
                    
                    return quarter, fiscal_year
                except ValueError:
                    continue
        except Exception:
            pass
    
    # Default to current quarter
    now = datetime.now()
    month = now.month
    year = now.year
    
    if month >= 4 and month <= 6:
        return "Q1", f"FY{year + 1}"
    elif month >= 7 and month <= 9:
        return "Q2", f"FY{year + 1}"
    elif month >= 10 and month <= 12:
        return "Q3", f"FY{year + 1}"
    else:
        return "Q4", f"FY{year}"

# =============================================================================
# GOOGLE DRIVE OPERATIONS
# =============================================================================

def get_drive_service():
    """Get Google Drive service."""
    credentials = get_google_credentials()
    return build("drive", "v3", credentials=credentials)

def get_or_create_folder(service, folder_name, parent_id):
    """Get existing folder or create new one."""
    # Search for existing folder
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and '{parent_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    
    if files:
        return files[0]["id"]
    
    # Create new folder
    metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    folder = service.files().create(body=metadata, fields="id").execute()
    logger.info(f"Created folder: {folder_name}")
    return folder["id"]

def file_exists_in_drive(service, filename, folder_id):
    """Check if file already exists in folder."""
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id)").execute()
    return len(results.get("files", [])) > 0

def upload_to_drive(service, file_content, filename, folder_id):
    """Upload file to Google Drive."""
    # Check if already exists
    if file_exists_in_drive(service, filename, folder_id):
        logger.info(f"  Skipped (exists): {filename}")
        return None
    
    metadata = {
        "name": filename,
        "parents": [folder_id]
    }
    
    media = MediaIoBaseUpload(BytesIO(file_content), mimetype="application/pdf")
    file = service.files().create(body=metadata, media_body=media, fields="id").execute()
    
    logger.info(f"  Uploaded: {filename}")
    return file["id"]

# =============================================================================
# DOWNLOAD AND UPLOAD TRANSCRIPTS
# =============================================================================

def download_and_upload_transcripts(transcripts, session, drive_service):
    """Download transcripts and upload to Google Drive."""
    
    stats = {"downloaded": 0, "skipped": 0, "failed": 0}
    
    for i, transcript in enumerate(transcripts, 1):
        try:
            company = transcript.get("company", "Unknown")
            pdf_url = transcript.get("pdf_url")
            
            if not pdf_url:
                continue
            
            # Determine quarter and fiscal year
            quarter, fiscal_year = determine_quarter_fy(
                transcript.get("date", ""),
                transcript.get("raw_text", "") or transcript.get("text", "")
            )
            
            # Use transcript's quarter/fy if available
            if transcript.get("quarter") and transcript.get("quarter") != "Unknown":
                quarter = transcript["quarter"]
            if transcript.get("fiscal_year") and transcript.get("fiscal_year") != "Unknown":
                fiscal_year = transcript["fiscal_year"]
            
            logger.info(f"[{i}/{len(transcripts)}] {company} - {fiscal_year} {quarter}")
            
            # Create folder structure: FY2025/Q3/
            fy_folder_id = get_or_create_folder(drive_service, fiscal_year, DRIVE_FOLDER_ID)
            quarter_folder_id = get_or_create_folder(drive_service, quarter, fy_folder_id)
            
            # Clean filename
            safe_company = re.sub(r'[<>:"/\\|?*]', '', company)
            filename = f"{safe_company} - {fiscal_year} {quarter} Transcript.pdf"
            
            # Check if already uploaded
            if file_exists_in_drive(drive_service, filename, quarter_folder_id):
                logger.info(f"  Skipped (exists): {filename}")
                stats["skipped"] += 1
                continue
            
            # Download PDF
            response = session.get(pdf_url, timeout=30)
            response.raise_for_status()
            
            # Upload to Drive
            upload_to_drive(drive_service, response.content, filename, quarter_folder_id)
            stats["downloaded"] += 1
            
            time.sleep(DOWNLOAD_DELAY)
            
        except Exception as e:
            logger.error(f"  Failed: {e}")
            stats["failed"] += 1
    
    return stats

# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("CONCALL TRANSCRIPT DOWNLOADER")
    logger.info("=" * 60)
    
    # Get credentials
    username = os.getenv("SCREENER_USERNAME")
    password = os.getenv("SCREENER_PASSWORD")
    
    if not username or not password:
        raise ValueError("SCREENER_USERNAME and SCREENER_PASSWORD required")
    
    # Initialize
    session = create_session()
    driver = create_webdriver()
    drive_service = get_drive_service()
    
    try:
        # Login
        if not login_to_screener(driver, username, password):
            raise RuntimeError("Failed to login")
        
        # Get transcripts from main transcripts page
        logger.info("\n" + "=" * 60)
        logger.info("SCRAPING TRANSCRIPTS")
        logger.info("=" * 60)
        
        transcripts = scrape_transcripts_page(driver, session)
        
        if not transcripts:
            logger.warning("No transcripts found on main page. Trying company-wise...")
            
            # Alternative: Get companies and scrape each
            companies = get_all_companies(driver)
            
            for i, company in enumerate(companies[:100], 1):  # Limit for testing
                logger.info(f"[{i}/{len(companies)}] Checking {company['name']}...")
                company_transcripts = get_company_transcripts(driver, company["url"], session)
                
                for t in company_transcripts:
                    t["company"] = company["name"]
                    transcripts.append(t)
                
                time.sleep(0.3)
        
        logger.info(f"\nTotal transcripts to process: {len(transcripts)}")
        
        # Download and upload
        logger.info("\n" + "=" * 60)
        logger.info("DOWNLOADING AND UPLOADING")
        logger.info("=" * 60)
        
        stats = download_and_upload_transcripts(transcripts, session, drive_service)
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Downloaded: {stats['downloaded']}")
        logger.info(f"Skipped (already exists): {stats['skipped']}")
        logger.info(f"Failed: {stats['failed']}")
        logger.info(f"Google Drive folder: https://drive.google.com/drive/folders/{DRIVE_FOLDER_ID}")
        
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
