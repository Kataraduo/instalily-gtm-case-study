"""Stakeholder Finder Module for DuPont Tedlar Sales Lead Generation System

This module is responsible for finding key stakeholders (decision makers)
at target companies.
"""

import os
import logging
import pandas as pd
import logging
import time
import random
from typing import List, Dict, Any
import requests
from pathlib import Path
import sys
import re
import time

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.config.config import (
    RAW_DATA_DIR, 
    PROCESSED_DATA_DIR, 
    OUTPUT_DATA_DIR,
    USER_AGENT,
    REQUEST_TIMEOUT,
    REQUEST_DELAY,
    HUNTER_API_KEY
)


class StakeholderFinder:
    """Class for finding key stakeholders at target companies"""
    
    def __init__(self):
        """Initialize the StakeholderFinder with default headers and settings"""
        self.headers = {
            'User-Agent': USER_AGENT,
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.timeout = REQUEST_TIMEOUT
        self.delay = REQUEST_DELAY
        
        # Ensure output directories exist
        self.output_dir = OUTPUT_DATA_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Define relevant job titles for decision makers in the graphics and signage industry
        self.relevant_titles = {
            'executive': [
                'CEO', 'Chief Executive Officer',
                'President', 
                'Owner', 'Founder', 'Co-Founder',
                'Managing Director', 
                'COO', 'Chief Operating Officer',
                'CFO', 'Chief Financial Officer',
                'CTO', 'Chief Technology Officer',
                'CMO', 'Chief Marketing Officer'
            ],
            'marketing': [
                'Marketing Director', 'Director of Marketing',
                'Marketing Manager', 'Brand Manager',
                'Marketing VP', 'VP of Marketing',
                'Creative Director'
            ],
            'operations': [
                'Operations Director', 'Director of Operations',
                'Operations Manager', 'Production Manager',
                'Operations VP', 'VP of Operations',
                'Facility Manager'
            ],
            'purchasing': [
                'Purchasing Director', 'Director of Purchasing',
                'Purchasing Manager', 'Procurement Manager',
                'Purchasing VP', 'VP of Purchasing',
                'Supply Chain Manager', 'Sourcing Manager'
            ],
            'technical': [
                'Technical Director', 'Director of Technology',
                'Technical Manager', 'Production Manager',
                'Engineering Manager', 'Project Manager',
                'R&D Manager', 'Innovation Manager'
            ],
            'sales': [
                'Sales Director', 'Director of Sales',
                'Sales Manager', 'Business Development Manager',
                'Sales VP', 'VP of Sales',
                'Account Manager', 'Client Relations Manager'
            ]
        }
    
    def find_stakeholders(self, companies_df):
        """Find key stakeholders at target companies
        
        Args:
            companies_df (pandas.DataFrame): DataFrame containing company information
            
        Returns:
            pandas.DataFrame: DataFrame containing stakeholder information
        """
        self.logger.info(f"Finding stakeholders for {len(companies_df)} companies")
        
        all_stakeholders = []
        
        for _, company in companies_df.iterrows():
            company_name = company['name']
            company_domain = self._extract_domain(company.get('website', ''))
            
            self.logger.info(f"Finding stakeholders for {company_name}")
            
            # Try to find stakeholders using Hunter.io API if available
            if HUNTER_API_KEY and company_domain:
                stakeholders = self._find_stakeholders_with_hunter(company_domain, company_name)
                if stakeholders:
                    all_stakeholders.extend(stakeholders)
                    self.logger.info(f"Found {len(stakeholders)} stakeholders for {company_name} using Hunter.io")
                    
                    # Respect rate limits
                    time.sleep(self.delay)
                    continue
            
            # If Hunter.io API is not available or no stakeholders found, generate synthetic stakeholders
            synthetic_stakeholders = self._generate_synthetic_stakeholders(company)
            all_stakeholders.extend(synthetic_stakeholders)
            self.logger.info(f"Generated {len(synthetic_stakeholders)} synthetic stakeholders for {company_name}")
        
        # Create DataFrame from all stakeholders
        stakeholders_df = pd.DataFrame(all_stakeholders)
        
        # Add unique ID for each stakeholder if not already present
        if 'id' not in stakeholders_df.columns:
            stakeholders_df['id'] = [f"STAKE-{i:04d}" for i in range(1, len(stakeholders_df) + 1)]
        
        # Save stakeholders data
        stakeholders_df.to_csv(self.output_dir / 'stakeholders.csv', index=False)
        self.logger.info(f"Saved {len(stakeholders_df)} stakeholders to stakeholders.csv")
        
        return stakeholders_df
    
    def _extract_domain(self, url):
        """Extract domain from website URL
        
        Args:
            url (str): Website URL
            
        Returns:
            str: Domain name
        """
        if not url or not isinstance(url, str):
            return ''
        
        # Remove protocol and www
        domain = url.lower()
        domain = re.sub(r'^https?://', '', domain)
        domain = re.sub(r'^www\.', '', domain)
        
        # Remove path and query string
        domain = domain.split('/')[0]
        
        return domain
    
    def _find_stakeholders_with_hunter(self, domain, company_name):
        """Find stakeholders using Hunter.io API
        
        Args:
            domain (str): Company domain name
            company_name (str): Company name
            
        Returns:
            list: List of dictionaries containing stakeholder information
        """
        if not HUNTER_API_KEY or not domain:
            return []
        
        try:
            url = f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={HUNTER_API_KEY}"
            
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'data' in data and 'emails' in data['data']:
                    stakeholders = []
                    
                    for email in data['data']['emails']:
                        # Extract relevant information
                        first_name = email.get('first_name', '')
                        last_name = email.get('last_name', '')
                        email_address = email.get('value', '')
                        position = email.get('position', '')
                        linkedin = email.get('linkedin', '')
                        
                        # Skip if no email or name
                        if not email_address or not (first_name or last_name):
                            continue
                        
                        # Create stakeholder record
                        stakeholder = {
                            'first_name': first_name,
                            'last_name': last_name,
                            'name': f"{first_name} {last_name}".strip(),
                            'email': email_address,
                            'title': position,
                            'company': company_name,
                            'linkedin_url': linkedin,
                            'source': 'Hunter.io API',
                            'decision_making_power': self._calculate_decision_power_from_title(position)
                        }
                        
                        stakeholders.append(stakeholder)
                    
                    return stakeholders
                
                self.logger.warning(f"No email data found for {company_name} in Hunter.io response")
                return []
            else:
                self.logger.warning(f"Hunter.io API returned status code {response.status_code} for {company_name}")
                return []
        
        except Exception as e:
            self.logger.error(f"Error finding stakeholders for {company_name} with Hunter.io: {str(e)}")
            return []
    
    def _calculate_decision_power_from_title(self, title):
        """Calculate decision-making power score from job title
        
        Args:
            title (str): Job title
            
        Returns:
            float: Decision-making power score (0-1)
        """
        if not title or not isinstance(title, str):
            return 0.5  # Default medium score
        
        title = title.lower()
        
        # Check against relevant titles
        for category, titles in self.relevant_titles.items():
            for relevant_title in titles:
                if relevant_title.lower() in title:
                    # Assign scores based on category
                    if category == 'executive':
                        return 0.9  # Highest decision power
                    elif category in ['purchasing', 'operations']:
                        return 0.8  # High decision power
                    elif category in ['technical', 'marketing']:
                        return 0.7  # Medium-high decision power
                    elif category == 'sales':
                        return 0.6  # Medium decision power
        
        # Default score for unknown titles
        return 0.5
    
    def _generate_synthetic_stakeholders(self, company):
        """Generate synthetic stakeholders for a company when real data is not available
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            list: List of dictionaries containing synthetic stakeholder information
        """
        company_name = company['name']
        stakeholders = []
        
        # Generate 2-4 synthetic stakeholders per company
        num_stakeholders = random.randint(2, 4)
        
        # Select random categories for stakeholders
        categories = random.sample(list(self.relevant_titles.keys()), min(num_stakeholders, len(self.relevant_titles)))
        
        for i, category in enumerate(categories):
            # Select a random title from the category
            title = random.choice(self.relevant_titles[category])
            
            # Generate a synthetic name
            first_name = f"FirstName{i+1}"
            last_name = f"LastName{i+1}"
            
            # Generate a synthetic email
            domain = self._extract_domain(company.get('website', ''))
            if not domain:
                domain = company_name.lower().replace(' ', '') + '.com'
            email = f"{first_name.lower()}.{last_name.lower()}@{domain}"
            
            # Create stakeholder record
            stakeholder = {
                'first_name': first_name,
                'last_name': last_name,
                'name': f"{first_name} {last_name}",
                'email': email,
                'title': title,
                'company': company_name,
                'linkedin_url': '',
                'source': 'Synthetic',
                'decision_making_power': self._calculate_decision_power_from_title(title)
            }
            
            stakeholders.append(stakeholder)
        
        return stakeholders