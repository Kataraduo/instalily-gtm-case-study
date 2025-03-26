"""
Startup script - Run the entire sales lead generation system
"""
import os
import sys
import logging
from pathlib import Path

# Add project root directory to Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.main import main
from src.dashboard.simple_dashboard import SimpleDashboard

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        # API密钥现在从.env文件加载，不需要在这里设置
        
        # Run main program
        dashboard_data = main()
        
        # Generate and open dashboard
        dashboard = SimpleDashboard()
        html_path = dashboard.generate_html_dashboard(dashboard_data)
        dashboard.open_dashboard(html_path)
        
        logger.info("System execution completed!")
    except Exception as e:
        logger.error(f"System execution error: {str(e)}")
        raise