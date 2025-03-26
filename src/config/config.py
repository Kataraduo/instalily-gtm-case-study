"""
Configuration file containing all parameters needed for the system
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# 加载.env文件（只需要一次）
load_dotenv()

# Project root directory
ROOT_DIR = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Data directories
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
OUTPUT_DATA_DIR = DATA_DIR / "output"

# Ensure directories exist
for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DATA_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# API configuration
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
LINKEDIN_API_KEY = os.environ.get("LINKEDIN_API_KEY", "")
CLAY_API_KEY = os.environ.get("CLAY_API_KEY", "")

# 打印API密钥的前10个字符，用于调试（实际使用时可以删除）
if OPENAI_API_KEY:
    print(f"OpenAI API密钥已加载，前10个字符: {OPENAI_API_KEY[:10]}...")
else:
    print("警告: OpenAI API密钥未设置")

# 其余配置保持不变
# Target company configuration
TARGET_INDUSTRY = "Graphics & Signage"
TARGET_PRODUCT = "DuPont Tedlar"

# ICP (Ideal Customer Profile) criteria
ICP_CRITERIA = {
    "industry_keywords": [
        "signage", "graphics", "large-format printing", "vehicle wraps", 
        "architectural graphics", "protective films", "outdoor displays",
        "digital printing", "visual communications"
    ],
    "company_size_min": 50,  # Minimum employee count
    "annual_revenue_min": 5000000,  # Minimum annual revenue (USD)
    "decision_maker_titles": [
        "VP of Product Development", "Director of Innovation", 
        "R&D Leader", "Chief Technology Officer", "Head of Product", 
        "Director of Procurement", "Purchasing Manager", "Materials Manager"
    ]
}

# Relevant industry events and associations
INDUSTRY_EVENTS = [
    "ISA Sign Expo", "PRINTING United", "FESPA Global Print Expo",
    "Graphics of the Americas", "SGIA Expo", "GlobalShop"
]

INDUSTRY_ASSOCIATIONS = [
    "International Sign Association", "Specialty Graphic Imaging Association",
    "Printing Industries of America", "FESPA", "Association for PRINT Technologies"
]

# Model configuration
LLM_MODEL = "gpt-4"
EMBEDDING_MODEL = "text-embedding-ada-002"

# Lead scoring thresholds
LEAD_SCORE_THRESHOLD = 70  # 0-100 score, leads above this score are considered high quality