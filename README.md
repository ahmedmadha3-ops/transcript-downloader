# 📄 Concall Transcript Downloader

Automatically download all concall transcripts from Screener.in and save them to Google Drive, organized by fiscal year and quarter.

## Features

- 🔍 Scrapes all transcripts from Screener.in
- 📥 Downloads PDFs automatically
- 📁 Organizes by FY/Quarter (e.g., FY2025/Q3/)
- ⏭️ Skips already downloaded files
- 🚀 Run manually via GitHub Actions

## Folder Structure

```
Concall Transcripts/
├── FY2025/
│   ├── Q1/
│   │   ├── Reliance Industries - FY2025 Q1 Transcript.pdf
│   │   └── TCS - FY2025 Q1 Transcript.pdf
│   ├── Q2/
│   ├── Q3/
│   └── Q4/
├── FY2026/
│   └── Q3/
└── ...
```

## Setup

### 1. Fork this repository

### 2. Add GitHub Secrets

Go to Settings → Secrets and variables → Actions, add:

| Secret | Description |
|--------|-------------|
| `SCREENER_USERNAME` | Screener.in email |
| `SCREENER_PASSWORD` | Screener.in password |
| `GOOGLE_CREDENTIALS_BASE64` | Base64-encoded service account JSON |
| `DRIVE_FOLDER_ID` | Google Drive folder ID |
| `EMAIL_USERNAME` | Gmail for notifications (optional) |
| `EMAIL_PASSWORD` | Gmail app password (optional) |
| `NOTIFICATION_EMAIL` | Email for notifications (optional) |

### 3. Share Google Drive Folder

Share your "Concall Transcripts" folder with the service account email.

### 4. Run!

Go to Actions → "Download Transcripts" → Run workflow

## Local Development

```bash
# Setup
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Set environment variables
export SCREENER_USERNAME='your_email'
export SCREENER_PASSWORD='your_password'
export DRIVE_FOLDER_ID='your_folder_id'

# Run
python transcript_downloader.py
```

## Notes

- First run may take a long time (many transcripts)
- Subsequent runs will skip already downloaded files
- Requires Screener.in Premium for transcript access
