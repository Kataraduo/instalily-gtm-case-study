"""Dashboard Generator Module for DuPont Tedlar Sales Lead Generation System

This module is responsible for generating an interactive dashboard to visualize
lead scoring results and provide insights for the sales team.
"""

import os
import logging
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path
import sys
import json

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.config.config import (
    OUTPUT_DATA_DIR,
    DASHBOARD_TITLE,
    DASHBOARD_DESCRIPTION
)


class DashboardGenerator:
    """Class for generating interactive dashboards to visualize lead scoring results"""
    
    def __init__(self):
        """Initialize the DashboardGenerator with default settings"""
        # Ensure output directories exist
        self.output_dir = OUTPUT_DATA_DIR
        self.dashboard_dir = self.output_dir / 'dashboard'
        self.dashboard_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Dashboard settings
        self.title = DASHBOARD_TITLE
        self.description = DASHBOARD_DESCRIPTION
        self.color_scale = px.colors.sequential.Viridis
    
    def generate_dashboard(self, leads_df, companies_df=None, stakeholders_df=None):
        """Generate an interactive dashboard to visualize lead scoring results
        
        Args:
            leads_df (pandas.DataFrame): DataFrame containing lead information
            companies_df (pandas.DataFrame, optional): DataFrame containing company information
            stakeholders_df (pandas.DataFrame, optional): DataFrame containing stakeholder information
            
        Returns:
            str: Path to the generated dashboard HTML file
        """
        self.logger.info("Generating interactive dashboard")
        
        # Create HTML content
        html_content = self._create_dashboard_html(leads_df, companies_df, stakeholders_df)
        
        # Save dashboard HTML file
        dashboard_path = self.output_dir / 'dashboard.html'
        with open(dashboard_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.info(f"Dashboard saved to {dashboard_path}")
        
        return str(dashboard_path)
    
    def _create_dashboard_html(self, leads_df, companies_df=None, stakeholders_df=None):
        """Create HTML content for the dashboard
        
        Args:
            leads_df (pandas.DataFrame): DataFrame containing lead information
            companies_df (pandas.DataFrame, optional): DataFrame containing company information
            stakeholders_df (pandas.DataFrame, optional): DataFrame containing stakeholder information
            
        Returns:
            str: HTML content for the dashboard
        """
        # Create dashboard components
        lead_table = self._create_lead_table(leads_df)
        lead_charts = self._create_lead_charts(leads_df)
        company_charts = self._create_company_charts(companies_df if companies_df is not None else leads_df)
        stakeholder_charts = self._create_stakeholder_charts(stakeholders_df if stakeholders_df is not None else leads_df)
        
        # Combine components into a complete HTML document
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{self.title}</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
            <script src="https://cdn.datatables.net/1.11.5/js/jquery.dataTables.min.js"></script>
            <link rel="stylesheet" href="https://cdn.datatables.net/1.11.5/css/jquery.dataTables.min.css">
            <style>
                body {{font-family: Arial, sans-serif; margin: 0; padding: 0; background-color: #f5f5f5;}}
                .container {{max-width: 1200px; margin: 0 auto; padding: 20px;}}
                .header {{background-color: #1a237e; color: white; padding: 20px; text-align: center; margin-bottom: 20px;}}
                .dashboard-section {{background-color: white; padding: 20px; margin-bottom: 20px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);}}
                .chart-container {{width: 100%; height: 400px; margin-bottom: 20px;}}
                .chart-row {{display: flex; flex-wrap: wrap; justify-content: space-between;}}
                .chart-half {{width: 48%; margin-bottom: 20px;}}
                h1, h2, h3 {{color: #1a237e;}}
                .tier-1 {{background-color: #c8e6c9; color: #2e7d32;}}
                .tier-2 {{background-color: #fff9c4; color: #f9a825;}}
                .tier-3 {{background-color: #ffcdd2; color: #c62828;}}
                .dataTables_wrapper {{margin-bottom: 30px;}}
                table.dataTable thead th {{background-color: #e8eaf6; color: #1a237e;}}
                @media (max-width: 768px) {{.chart-half {{width: 100%;}}}}
                
                /* Styles for expandable cells */
                .expandable-cell {{position: relative;}}
                .preview-text {{color: #555;}}
                .expand-btn {{cursor: pointer; color: #1a237e; text-decoration: underline; margin-top: 5px; font-size: 0.9em;}}
                .full-text {{margin-top: 5px; padding: 10px; background-color: #f9f9f9; border-radius: 4px; border-left: 3px solid #1a237e;}}
                
                /* Qualification details styling */
                .qualification-section {{margin-bottom: 8px;}}
                .qualification-section h4 {{margin: 5px 0; color: #1a237e; font-size: 0.9em;}}
                .qualification-section p {{margin: 3px 0; font-size: 0.9em;}}
                
                /* Outreach message styling */
                .outreach-message {{font-style: italic; color: #333;}}
                .decision-maker-info {{margin-bottom: 8px; font-weight: bold;}}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{self.title}</h1>
                <p>{self.description}</p>
            </div>
            
            <div class="container">
                <div class="dashboard-section">
                    <h2>Lead Prioritization</h2>
                    <p>Below are the prioritized leads for DuPont Tedlar's Graphics & Signage team, ranked by lead score.</p>
                    {lead_table}
                </div>
                
                <div class="dashboard-section">
                    <h2>Lead Overview</h2>
                    <div class="chart-row">
                        {lead_charts}
                    </div>
                </div>
                
                <div class="dashboard-section">
                    <h2>Company Insights</h2>
                    <div class="chart-row">
                        {company_charts}
                    </div>
                </div>
                
                <div class="dashboard-section">
                    <h2>Stakeholder Analysis</h2>
                    <div class="chart-row">
                        {stakeholder_charts}
                    </div>
                </div>
            </div>
            
            <script>
                $(document).ready(function() {{                    
                    $('#leads-table').DataTable({{                        
                        pageLength: 10,
                        order: [[2, 'desc']],
                        columnDefs: [
                            {{targets: 3, render: function(data, type, row) {{
                                if (data === 'Tier 1') return '<span class="tier-1">Tier 1</span>';
                                if (data === 'Tier 2') return '<span class="tier-2">Tier 2</span>';
                                if (data === 'Tier 3') return '<span class="tier-3">Tier 3</span>';
                                return data;
                            }}}}
                        ]
                    }});
                    
                    // Add event listeners for expandable cells
                    $(document).on('click', '.expand-btn', function() {{
                        var fullText = $(this).siblings('.full-text');
                        var previewText = $(this).siblings('.preview-text');
                        
                        if (fullText.is(':visible')) {{
                            fullText.hide();
                            previewText.show();
                            $(this).text('Show More');
                        }} else {{
                            fullText.show();
                            previewText.hide();
                            $(this).text('Show Less');
                        }}
                    }});
                }});
            </script>
        </body>
        </html>
        """
        
        return html_content
    
    def _create_lead_table(self, leads_df):
        """Create an HTML table for leads
        
        Args:
            leads_df (pandas.DataFrame): DataFrame containing lead information
            
        Returns:
            str: HTML table for leads
        """
        # Select and rename columns for the table
        table_columns = {
            'name': 'Stakeholder',
            'title': 'Title',
            'lead_score': 'Lead Score',
            'tier': 'Tier',
            'company': 'Company',
            'industry': 'Industry',
            'company_size': 'Company Size',
            'email': 'Email',
            'qualification_rationale': 'Qualification Rationale',
            'personalized_outreach': 'Personalized Outreach'
        }
        
        # Filter columns that exist in the DataFrame
        existing_columns = [col for col in table_columns.keys() if col in leads_df.columns]
        
        # Create a copy with only the selected columns
        table_df = leads_df[existing_columns].copy()
        
        # Rename columns
        table_df.columns = [table_columns[col] for col in existing_columns]
        
        # Convert DataFrame to HTML table
        html_table = f"""
        <table id="leads-table" class="display" style="width:100%">
            <thead>
                <tr>
                    {' '.join(f'<th>{col}</th>' for col in table_df.columns)}
                </tr>
            </thead>
            <tbody>
                {self._df_to_table_rows(table_df)}
            </tbody>
        </table>
        """
        
        return html_table
    
    def _df_to_table_rows(self, df):
        """Convert DataFrame to HTML table rows with expandable details for qualification rationale and outreach
        
        Args:
            df (pandas.DataFrame): DataFrame to convert
            
        Returns:
            str: HTML table rows
        """
        rows = []
        for _, row in df.iterrows():
            cells = []
            
            for i, value in enumerate(row.values):
                col_name = df.columns[i]
                
                # Handle qualification rationale and personalized outreach with expandable sections
                if col_name in ['Qualification Rationale', 'Personalized Outreach'] and value:
                    # Create expandable content for longer text
                    cells.append(f'''
                    <td>
                        <div class="expandable-cell">
                            <div class="preview-text">{str(value)[:50]}...</div>
                            <div class="expand-btn">Show More</div>
                            <div class="full-text" style="display:none">{value}</div>
                        </div>
                    </td>
                    ''')
                else:
                    cells.append(f'<td>{value}</td>')
                    
            rows.append(f'<tr>{"".join(cells)}</tr>')
        
        return '\n'.join(rows)
    
    def _create_lead_charts(self, leads_df):
        """Create charts for lead overview
        
        Args:
            leads_df (pandas.DataFrame): DataFrame containing lead information
            
        Returns:
            str: HTML content for lead charts
        """
        # Create tier distribution chart
        if 'tier' in leads_df.columns:
            tier_counts = leads_df['tier'].value_counts().reset_index()
            tier_counts.columns = ['Tier', 'Count']
            
            tier_fig = px.pie(
                tier_counts, 
                values='Count', 
                names='Tier', 
                title='Lead Distribution by Tier',
                color='Tier',
                color_discrete_map={
                    'Tier 1': '#4caf50',
                    'Tier 2': '#ffeb3b',
                    'Tier 3': '#f44336'
                }
            )
            tier_fig.update_traces(textposition='inside', textinfo='percent+label')
            tier_chart = tier_fig.to_html(full_html=False, include_plotlyjs=False)
        else:
            tier_chart = "<p>No tier data available</p>"
        
        # Create lead score distribution chart
        if 'lead_score' in leads_df.columns:
            score_fig = px.histogram(
                leads_df, 
                x='lead_score', 
                nbins=10,
                title='Lead Score Distribution',
                color_discrete_sequence=[self.color_scale[3]]
            )
            score_fig.update_layout(xaxis_title='Lead Score', yaxis_title='Number of Leads')
            score_chart = score_fig.to_html(full_html=False, include_plotlyjs=False)
        else:
            score_chart = "<p>No lead score data available</p>"
        
        # Combine charts into HTML
        html_content = f"""
        <div class="chart-half">
            {tier_chart}
        </div>
        <div class="chart-half">
            {score_chart}
        </div>
        """
        
        return html_content
    
    def _create_company_charts(self, df):
        """Create charts for company insights
        
        Args:
            df (pandas.DataFrame): DataFrame containing company information
            
        Returns:
            str: HTML content for company charts
        """
        # Create industry distribution chart
        if 'industry' in df.columns:
            industry_counts = df['industry'].value_counts().reset_index()
            industry_counts.columns = ['Industry', 'Count']
            
            # Limit to top 10 industries for readability
            if len(industry_counts) > 10:
                other_count = industry_counts.iloc[10:]['Count'].sum()
                industry_counts = industry_counts.iloc[:10]
                industry_counts = pd.concat([industry_counts, pd.DataFrame([{'Industry': 'Other', 'Count': other_count}])])
            
            industry_fig = px.bar(
                industry_counts, 
                x='Industry', 
                y='Count', 
                title='Companies by Industry',
                color='Count',
                color_continuous_scale=self.color_scale
            )
            industry_fig.update_layout(xaxis_title='Industry', yaxis_title='Number of Companies')
            industry_chart = industry_fig.to_html(full_html=False, include_plotlyjs=False)
        else:
            industry_chart = "<p>No industry data available</p>"
        
        # Create company size distribution chart
        if 'company_size' in df.columns:
            size_counts = df['company_size'].value_counts().reset_index()
            size_counts.columns = ['Company Size', 'Count']
            
            size_fig = px.pie(
                size_counts, 
                values='Count', 
                names='Company Size', 
                title='Companies by Size',
                color='Company Size',
                color_discrete_sequence=self.color_scale
            )
            size_fig.update_traces(textposition='inside', textinfo='percent+label')
            size_chart = size_fig.to_html(full_html=False, include_plotlyjs=False)
        else:
            size_chart = "<p>No company size data available</p>"
        
        # Combine charts into HTML
        html_content = f"""
        <div class="chart-half">
            {industry_chart}
        </div>
        <div class="chart-half">
            {size_chart}
        </div>
        """
        
        return html_content
    
    def _create_stakeholder_charts(self, df):
        """Create charts for stakeholder analysis
        
        Args:
            df (pandas.DataFrame): DataFrame containing stakeholder information
            
        Returns:
            str: HTML content for stakeholder charts
        """
        # Create decision power vs company score scatter plot
        if 'decision_making_power' in df.columns and 'company_score' in df.columns:
            scatter_fig = px.scatter(
                df, 
                x='decision_making_power', 
                y='company_score',
                color='lead_score' if 'lead_score' in df.columns else None,
                size='lead_score' if 'lead_score' in df.columns else None,
                hover_name='name' if 'name' in df.columns else None,
                hover_data=['company', 'title'] if all(col in df.columns for col in ['company', 'title']) else None,
                title='Stakeholder Decision Power vs Company Score',
                color_continuous_scale=self.color_scale
            )
            scatter_fig.update_layout(
                xaxis_title='Decision Making Power',
                yaxis_title='Company Score',
                xaxis=dict(range=[0, 1]),
                yaxis=dict(range=[0, 1])
            )
            scatter_chart = scatter_fig.to_html(full_html=False, include_plotlyjs=False)
        else:
            scatter_chart = "<p>No stakeholder scoring data available</p>"
        
        # Create job title distribution chart
        if 'title' in df.columns:
            # Extract job categories from titles
            def categorize_title(title):
                title = str(title).lower()
                if any(keyword in title for keyword in ['ceo', 'chief', 'president', 'owner', 'founder']):
                    return 'Executive'
                elif any(keyword in title for keyword in ['director', 'vp', 'vice president', 'head']):
                    return 'Director/VP'
                elif any(keyword in title for keyword in ['manager', 'lead', 'senior']):
                    return 'Manager'
                else:
                    return 'Other'
            
            df['job_category'] = df['title'].apply(categorize_title)
            title_counts = df['job_category'].value_counts().reset_index()
            title_counts.columns = ['Job Category', 'Count']
            
            title_fig = px.pie(
                title_counts, 
                values='Count', 
                names='Job Category', 
                title='Stakeholders by Job Level',
                color='Job Category',
                color_discrete_sequence=self.color_scale
            )
            title_fig.update_traces(textposition='inside', textinfo='percent+label')
            title_chart = title_fig.to_html(full_html=False, include_plotlyjs=False)
        else:
            title_chart = "<p>No job title data available</p>"
        
        # Combine charts into HTML
        html_content = f"""
        <div class="chart-half">
            {scatter_chart}
        </div>
        <div class="chart-half">
            {title_chart}
        </div>
        """
        
        return html_content