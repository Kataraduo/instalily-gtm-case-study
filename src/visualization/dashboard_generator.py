"""
Dashboard generation module for visualizing outreach data
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import logging
from pathlib import Path
from typing import Dict, Any, List
from ..config.config import OUTPUT_DATA_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DashboardGenerator:
    """Dashboard generation class for outreach data visualization"""
    
    def __init__(self, output_dir: Path = None):
        """
        Initialize dashboard generator
        
        Args:
            output_dir: Directory to save dashboard files, defaults to OUTPUT_DATA_DIR
        """
        self.output_dir = output_dir or OUTPUT_DATA_DIR
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        # Set visualization style
        sns.set_style("whitegrid")
        plt.rcParams.update({'font.size': 12})
    
    def generate_dashboard(self, stakeholders_df: pd.DataFrame, companies_df: pd.DataFrame) -> str:
        """
        Generate dashboard for outreach data
        
        Args:
            stakeholders_df: Stakeholders DataFrame with outreach messages
            companies_df: Companies DataFrame
            
        Returns:
            Path to generated dashboard HTML file
        """
        logger.info("Generating outreach dashboard")
        
        # Create output directory
        dashboard_dir = self.output_dir / "dashboard"
        os.makedirs(dashboard_dir, exist_ok=True)
        
        # Generate various charts
        self._generate_company_distribution(companies_df, dashboard_dir)
        self._generate_stakeholder_analysis(stakeholders_df, dashboard_dir)
        self._generate_message_stats(stakeholders_df, dashboard_dir)
        
        # Create HTML dashboard
        dashboard_path = self._create_html_dashboard(stakeholders_df, companies_df, dashboard_dir)
        
        logger.info(f"Dashboard generated at: {dashboard_path}")
        return dashboard_path
    
    def _generate_company_distribution(self, companies_df: pd.DataFrame, output_dir: Path) -> None:
        """Generate company distribution charts"""
        plt.figure(figsize=(10, 6))
        
        # Industry distribution
        if 'industry' in companies_df.columns:
            industry_counts = companies_df['industry'].value_counts()
            plt.subplot(1, 2, 1)
            industry_counts.plot(kind='bar')
            plt.title('Company Industry Distribution')
            plt.xlabel('Industry')
            plt.ylabel('Number of Companies')
            plt.xticks(rotation=45)
        
        # Company size distribution
        if 'company_size' in companies_df.columns:
            plt.subplot(1, 2, 2)
            companies_df['company_size'].hist(bins=10)
            plt.title('Company Size Distribution')
            plt.xlabel('Number of Employees')
            plt.ylabel('Number of Companies')
        
        plt.tight_layout()
        plt.savefig(output_dir / "company_distribution.png")
        plt.close()
    
    def _generate_stakeholder_analysis(self, stakeholders_df: pd.DataFrame, output_dir: Path) -> None:
        """Generate stakeholder analysis charts"""
        plt.figure(figsize=(10, 6))
        
        # Title distribution
        if 'title' in stakeholders_df.columns:
            title_counts = stakeholders_df['title'].value_counts().head(10)
            plt.subplot(1, 2, 1)
            title_counts.plot(kind='bar')
            plt.title('Stakeholder Title Distribution (Top 10)')
            plt.xlabel('Title')
            plt.ylabel('Count')
            plt.xticks(rotation=45)
        
        # Decision making power distribution
        if 'decision_making_power' in stakeholders_df.columns:
            plt.subplot(1, 2, 2)
            decision_counts = stakeholders_df['decision_making_power'].value_counts()
            decision_counts.plot(kind='pie', autopct='%1.1f%%')
            plt.title('Decision Making Power Distribution')
            plt.ylabel('')
        
        plt.tight_layout()
        plt.savefig(output_dir / "stakeholder_analysis.png")
        plt.close()
    
    def _generate_message_stats(self, stakeholders_df: pd.DataFrame, output_dir: Path) -> None:
        """Generate message statistics charts"""
        if 'outreach_message' not in stakeholders_df.columns:
            return
        
        plt.figure(figsize=(10, 6))
        
        # Calculate message length
        stakeholders_df['message_length'] = stakeholders_df['outreach_message'].apply(len)
        
        # Message length distribution
        plt.subplot(1, 2, 1)
        stakeholders_df['message_length'].hist(bins=10)
        plt.title('Message Length Distribution')
        plt.xlabel('Character Count')
        plt.ylabel('Number of Messages')
        
        # Number of messages per company
        plt.subplot(1, 2, 2)
        company_message_counts = stakeholders_df.groupby('company').size()
        company_message_counts.plot(kind='bar')
        plt.title('Number of Outreach Messages per Company')
        plt.xlabel('Company')
        plt.ylabel('Number of Messages')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig(output_dir / "message_stats.png")
        plt.close()
    
    def _create_html_dashboard(self, stakeholders_df: pd.DataFrame, companies_df: pd.DataFrame, output_dir: Path) -> str:
        """Create HTML dashboard"""
        dashboard_path = output_dir / "outreach_dashboard.html"
        
        # Calculate some statistics
        total_companies = len(companies_df)
        total_stakeholders = len(stakeholders_df)
        messages_generated = stakeholders_df['outreach_message'].notna().sum()
        
        # Create HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Outreach Campaign Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                .dashboard {{ max-width: 1200px; margin: 0 auto; }}
                .header {{ background-color: #f8f9fa; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
                .stats-container {{ display: flex; justify-content: space-between; margin-bottom: 20px; }}
                .stat-box {{ background-color: #ffffff; border: 1px solid #dee2e6; border-radius: 5px; padding: 15px; width: 30%; }}
                .chart-container {{ margin-bottom: 30px; }}
                h1, h2 {{ color: #343a40; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
                th, td {{ border: 1px solid #dee2e6; padding: 8px; text-align: left; }}
                th {{ background-color: #f8f9fa; }}
                tr:nth-child(even) {{ background-color: #f8f9fa; }}
            </style>
        </head>
        <body>
            <div class="dashboard">
                <div class="header">
                    <h1>Outreach Campaign Dashboard</h1>
                    <p>Generated on: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div class="stats-container">
                    <div class="stat-box">
                        <h3>Target Companies</h3>
                        <h2>{total_companies}</h2>
                    </div>
                    <div class="stat-box">
                        <h3>Stakeholders</h3>
                        <h2>{total_stakeholders}</h2>
                    </div>
                    <div class="stat-box">
                        <h3>Messages Generated</h3>
                        <h2>{messages_generated}</h2>
                    </div>
                </div>
                
                <div class="chart-container">
                    <h2>Company Analysis</h2>
                    <img src="company_distribution.png" alt="Company Distribution" style="width: 100%;">
                </div>
                
                <div class="chart-container">
                    <h2>Stakeholder Analysis</h2>
                    <img src="stakeholder_analysis.png" alt="Stakeholder Analysis" style="width: 100%;">
                </div>
                
                <div class="chart-container">
                    <h2>Message Statistics</h2>
                    <img src="message_stats.png" alt="Message Statistics" style="width: 100%;">
                </div>
                
                <div class="chart-container">
                    <h2>Stakeholder List</h2>
                    <table>
                        <tr>
                            <th>Name</th>
                            <th>Title</th>
                            <th>Company</th>
                            <th>Decision Making Power</th>
                        </tr>
        """
        
        # Add stakeholder data
        for _, row in stakeholders_df.head(10).iterrows():
            html_content += f"""
                        <tr>
                            <td>{row.get('name', '')}</td>
                            <td>{row.get('title', '')}</td>
                            <td>{row.get('company', '')}</td>
                            <td>{row.get('decision_making_power', '')}</td>
                        </tr>
            """
        
        # If there are more than 10 stakeholders, add a note
        if len(stakeholders_df) > 10:
            html_content += f"""
                        <tr>
                            <td colspan="4">... and {len(stakeholders_df) - 10} more stakeholders</td>
                        </tr>
            """
        
        # Complete HTML
        html_content += """
                    </table>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Write HTML file
        with open(dashboard_path, 'w') as f:
            f.write(html_content)
        
        return str(dashboard_path)