"""Configuration settings for the DuPont Tedlar Sales Lead Generation System"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project directories
PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_DIR = PROJECT_ROOT / "src"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
OUTPUT_DATA_DIR = DATA_DIR / "output"

# Ensure all data directories exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DATA_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# API Keys and Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
CLEARBIT_API_KEY = os.getenv("CLEARBIT_API_KEY")
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")

# Industry-specific configuration
TARGET_INDUSTRY = "Graphics & Signage"
TARGET_PRODUCT = "DuPont Tedlar"

# Web scraping configuration
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
REQUEST_TIMEOUT = 30  # seconds
REQUEST_DELAY = 2  # seconds between requests

# Dashboard configuration
DASHBOARD_TITLE = "DuPont Tedlar Sales Lead Dashboard"
DASHBOARD_DESCRIPTION = "Interactive dashboard for DuPont Tedlar's Graphics & Signage sales team to prioritize and manage leads."

# Scoring weights for lead prioritization
SCORING_WEIGHTS = {
    'company_size': 0.3,
    'industry_relevance': 0.3,
    'product_fit': 0.4,
    'decision_making_power': 0.5,
    'engagement_level': 0.3,
    'communication_history': 0.2
}