"""
Simple Dashboard Module - For displaying sales lead data
"""
import pandas as pd
import logging
import os
from pathlib import Path
import webbrowser
import json
from ..config.config import OUTPUT_DATA_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleDashboard:
    """Simple Dashboard Class"""
    
    def __init__(self, data_path=None):
        """
        Initialize dashboard
        
        Args:
            data_path: Data file path, defaults to None, will use path from configuration
        """
        if data_path is None:
            self.data_path = OUTPUT_DATA_DIR / "dashboard_data.csv"
        else:
            self.data_path = Path(data_path)
            
        self.html_output_path = OUTPUT_DATA_DIR / "dashboard.html"
    
    def load_data(self) -> pd.DataFrame:
        """
        Load dashboard data
        
        Returns:
            Dashboard data DataFrame
        """
        if not self.data_path.exists():
            logger.error(f"Data file does not exist: {self.data_path}")
            return pd.DataFrame()
            
        try:
            data = pd.read_csv(self.data_path)
            logger.info(f"Successfully loaded data: {len(data)} records")
            return data
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return pd.DataFrame()
    
    def generate_html_dashboard(self, data: pd.DataFrame = None) -> str:
        """
        Generate HTML dashboard
        
        Args:
            data: Dashboard data DataFrame, if None then load data
            
        Returns:
            HTML dashboard file path
        """
        if data is None:
            data = self.load_data()
            
        if data.empty:
            logger.error("No data available for display")
            return ""
        
        # Prepare data
        dashboard_data = data.sort_values(by='priority_score', ascending=False)
        
        # Generate HTML
        html_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>DuPont Tedlar Sales Lead Dashboard</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 20px;
                    background-color: #f5f5f5;
                }
                .container {
                    max-width: 1200px;
                    margin: 0 auto;
                    background-color: white;
                    padding: 20px;
                    border-radius: 5px;
                    box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                }
                h1 {
                    color: #003366;
                    text-align: center;
                }
                .summary {
                    background-color: #e6f2ff;
                    padding: 15px;
                    border-radius: 5px;
                    margin-bottom: 20px;
                }
                .lead-card {
                    border: 1px solid #ddd;
                    border-radius: 5px;
                    padding: 15px;
                    margin-bottom: 15px;
                    background-color: white;
                }
                .lead-card:hover {
                    box-shadow: 0 2px 8px rgba(0,0,0,0.2);
                }
                .lead-header {
                    display: flex;
                    justify-content: space-between;
                    border-bottom: 1px solid #eee;
                    padding-bottom: 10px;
                    margin-bottom: 10px;
                }
                .lead-name {
                    font-weight: bold;
                    font-size: 18px;
                    color: #003366;
                }
                .lead-score {
                    background-color: #003366;
                    color: white;
                    padding: 3px 8px;
                    border-radius: 12px;
                    font-size: 14px;
                }
                .lead-details {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 10px;
                }
                .lead-company {
                    font-weight: bold;
                }
                .lead-message {
                    background-color: #f9f9f9;
                    padding: 10px;
                    border-radius: 5px;
                    margin-top: 10px;
                    white-space: pre-line;
                }
                .lead-links {
                    margin-top: 10px;
                }
                .lead-links a {
                    color: #0066cc;
                    text-decoration: none;
                    margin-right: 15px;
                }
                .lead-links a:hover {
                    text-decoration: underline;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>DuPont Tedlar Sales Lead Dashboard</h1>
                
                <div class="summary">
                    <h2>Summary</h2>
                    <p>Total potential sales leads discovered: <strong>""" + str(len(dashboard_data)) + """</strong></p>
                    <p>High priority leads (score > 80): <strong>""" + str(len(dashboard_data[dashboard_data['priority_score'] > 80])) + """</strong></p>
                    <p>Medium priority leads (score 60-80): <strong>""" + str(len(dashboard_data[(dashboard_data['priority_score'] > 60) & (dashboard_data['priority_score'] <= 80)])) + """</strong></p>
                    <p>Low priority leads (score < 60): <strong>""" + str(len(dashboard_data[dashboard_data['priority_score'] <= 60])) + """</strong></p>
                </div>
                
                <h2>High Priority Sales Leads</h2>
        """
        
        # Add each sales lead card
        for _, lead in dashboard_data.head(10).iterrows():
            html_content += f"""
                <div class="lead-card">
                    <div class="lead-header">
                        <div class="lead-name">{lead['name']}</div>
                        <div class="lead-score">Score: {int(lead['priority_score'])}</div>
                    </div>
                    <div class="lead-details">
                        <div>
                            <div><strong>Title:</strong> {lead['title']}</div>
                            <div><strong>Company:</strong> {lead['company']}</div>
                            <div><strong>Decision-making power:</strong> {lead['decision_making_power']}</div>
                        </div>
                        <div>
                            <div><strong>Email:</strong> {lead['email']}</div>
                            <div><strong>Company ICP Score:</strong> {int(lead['company_icp_score'])}</div>
                            <div><strong>Relevance Score:</strong> {lead['relevance_score']}</div>
                        </div>
                    </div>
                    <div class="lead-message">
                        <strong>Suggested Outreach Message:</strong><br>
                        {lead['outreach_message']}
                    </div>
                    <div class="lead-links">
                        <a href="{lead['linkedin_url']}" target="_blank">LinkedIn Profile</a>
                        <a href="mailto:{lead['email']}">Send Email</a>
                    </div>
                </div>
            """
        
        html_content += """
            </div>
        </body>
        </html>
        """
        
        # Save HTML file
        with open(self.html_output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
            
        logger.info(f"Dashboard generated: {self.html_output_path}")
        return str(self.html_output_path)
    
    def open_dashboard(self, html_path: str = None):
        """
        Open dashboard in browser
        
        Args:
            html_path: HTML file path, if None then use default path
        """
        if html_path is None:
            html_path = str(self.html_output_path)
            
        if not os.path.exists(html_path):
            logger.error(f"Dashboard file does not exist: {html_path}")
            return
            
        # Open browser
        webbrowser.open('file://' + os.path.abspath(html_path))
        logger.info(f"Dashboard opened in browser: {html_path}")