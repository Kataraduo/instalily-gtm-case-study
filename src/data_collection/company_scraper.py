"""
Company data collection module
"""
import pandas as pd
import logging
import time
import random
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup
from ..config.config import ICP_CRITERIA, TARGET_INDUSTRY

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompanyScraper:
    """Company data collection class"""
    
    def __init__(self):
        """Initialize company scraper"""
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.industry_keywords = ICP_CRITERIA['industry_keywords']
    
    def collect_companies_data(self, events_df: pd.DataFrame, associations_df: pd.DataFrame) -> pd.DataFrame:
        """
        Collect company data from events and associations
        
        Args:
            events_df: Events DataFrame
            associations_df: Associations DataFrame
            
        Returns:
            Companies DataFrame
        """
        logger.info("Collecting company data from events and associations")
        
        companies = []
        
        # Collect companies from events
        if not events_df.empty:
            for _, event in events_df.iterrows():
                try:
                    event_companies = self._get_companies_from_event(event)
                    companies.extend(event_companies)
                    # Add random delay to avoid rate limiting
                    time.sleep(random.uniform(1, 3))
                except Exception as e:
                    logger.error(f"Error collecting companies from event {event['name']}: {str(e)}")
        
        # Collect companies from associations
        if not associations_df.empty:
            for _, association in associations_df.iterrows():
                try:
                    association_companies = self._get_companies_from_association(association)
                    companies.extend(association_companies)
                    # Add random delay to avoid rate limiting
                    time.sleep(random.uniform(1, 3))
                except Exception as e:
                    logger.error(f"Error collecting companies from association {association['name']}: {str(e)}")
        
        # Add mock data if no real data available
        if len(companies) < 10:
            logger.warning("Insufficient real data, adding mock company data")
            companies.extend(self._get_mock_companies(10 - len(companies)))
        
        # Create DataFrame and remove duplicates
        companies_df = pd.DataFrame(companies)
        companies_df = companies_df.drop_duplicates(subset=['name'])
        
        logger.info(f"Collected data for {len(companies_df)} companies")
        return companies_df
    
    def _get_companies_from_event(self, event: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get companies from event
        
        Args:
            event: Event information dictionary
            
        Returns:
            List of company dictionaries
        """
        logger.info(f"Getting companies from event: {event['name']}")
        
        companies = []
        
        # In a real implementation, this would scrape the event website for exhibitor lists
        # For this case study, we'll generate mock data based on the event
        
        # Mock implementation
        num_companies = random.randint(3, 8)
        for i in range(num_companies):
            company_name = f"{random.choice(['Advanced', 'Premier', 'Elite', 'Global', 'Pro'])} {random.choice(['Graphics', 'Signage', 'Displays', 'Printing', 'Visual'])} {random.choice(['Solutions', 'Systems', 'Inc', 'Co', 'Group'])}"
            
            company = {
                'name': company_name,
                'website': f"https://www.{company_name.lower().replace(' ', '')}.com",
                'industry': TARGET_INDUSTRY,
                'source': f"Event: {event['name']}",
                'source_url': event.get('url', ''),
                'products': [random.choice(['Signage', 'Banners', 'Vehicle Wraps', 'Displays', 'Graphics'])],
                'target_markets': [random.choice(['Retail', 'Corporate', 'Events', 'Outdoor', 'Transportation'])],
                'company_size': random.randint(20, 500),
                'annual_revenue': random.randint(1000000, 50000000)
            }
            
            companies.append(company)
        
        return companies
    
    def _get_companies_from_association(self, association: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Get companies from association
        
        Args:
            association: Association information dictionary
            
        Returns:
            List of company dictionaries
        """
        logger.info(f"Getting companies from association: {association['name']}")
        
        companies = []
        
        # In a real implementation, this would scrape the association website for member lists
        # For this case study, we'll generate mock data based on the association
        
        # Mock implementation
        num_companies = random.randint(5, 12)
        for i in range(num_companies):
            company_name = f"{random.choice(['Creative', 'Innovative', 'Dynamic', 'Modern', 'Tech'])} {random.choice(['Signs', 'Graphics', 'Imaging', 'Visuals', 'Media'])} {random.choice(['Partners', 'Experts', 'Professionals', 'Team', 'Studio'])}"
            
            company = {
                'name': company_name,
                'website': f"https://www.{company_name.lower().replace(' ', '')}.com",
                'industry': TARGET_INDUSTRY,
                'source': f"Association: {association['name']}",
                'source_url': association.get('url', ''),
                'products': [random.choice(['Digital Printing', 'Large Format', 'Architectural Graphics', 'Fleet Graphics', 'Retail Displays'])],
                'target_markets': [random.choice(['Retail', 'Corporate', 'Events', 'Outdoor', 'Transportation'])],
                'company_size': random.randint(20, 500),
                'annual_revenue': random.randint(1000000, 50000000)
            }
            
            companies.append(company)
        
        return companies
    
    def _get_mock_companies(self, num_companies: int) -> List[Dict[str, Any]]:
        """
        Generate mock company data
        
        Args:
            num_companies: Number of mock companies to generate
            
        Returns:
            List of mock company dictionaries
        """
        logger.info(f"Generating {num_companies} mock companies")
        
        companies = []
        
        company_name_prefixes = ['Advanced', 'Premier', 'Elite', 'Global', 'Pro', 'Creative', 'Innovative', 'Dynamic', 'Modern', 'Tech']
        company_name_mids = ['Graphics', 'Signage', 'Displays', 'Printing', 'Visual', 'Signs', 'Imaging', 'Visuals', 'Media']
        company_name_suffixes = ['Solutions', 'Systems', 'Inc', 'Co', 'Group', 'Partners', 'Experts', 'Professionals', 'Team', 'Studio']
        
        products_list = ['Signage', 'Banners', 'Vehicle Wraps', 'Displays', 'Graphics', 'Digital Printing', 'Large Format', 'Architectural Graphics', 'Fleet Graphics', 'Retail Displays']
        markets_list = ['Retail', 'Corporate', 'Events', 'Outdoor', 'Transportation', 'Hospitality', 'Education', 'Healthcare', 'Government', 'Entertainment']
        
        for i in range(num_companies):
            prefix = random.choice(company_name_prefixes)
            mid = random.choice(company_name_mids)
            suffix = random.choice(company_name_suffixes)
            
            company_name = f"{prefix} {mid} {suffix}"
            
            # Select 1-3 random products
            num_products = random.randint(1, 3)
            products = random.sample(products_list, num_products)
            
            # Select 1-3 random target markets
            num_markets = random.randint(1, 3)
            markets = random.sample(markets_list, num_markets)
            
            company = {
                'name': company_name,
                'website': f"https://www.{company_name.lower().replace(' ', '')}.com",
                'industry': TARGET_INDUSTRY,
                'source': 'Mock Data',
                'source_url': '',
                'products': products,
                'target_markets': markets,
                'company_size': random.randint(20, 500),
                'annual_revenue': random.randint(1000000, 50000000)
            }
            
            companies.append(company)
        
        return companies