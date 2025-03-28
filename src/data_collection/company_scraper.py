"""Company Scraper Module for DuPont Tedlar Sales Lead Generation System

This module is responsible for scraping company information from event websites
and industry association directories.
"""

import os
import re
import time
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.config.config import (
    RAW_DATA_DIR, 
    PROCESSED_DATA_DIR, 
    OUTPUT_DATA_DIR,
    USER_AGENT,
    REQUEST_TIMEOUT,
    REQUEST_DELAY
)


class CompanyScraper:
    """Class for scraping company information from event websites and association directories"""
    
    def __init__(self):
        """Initialize the CompanyScraper with default headers and settings"""
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
    
    def collect_companies_data(self, events_df, associations_df):
        """Collect company data from events and associations
        
        Args:
            events_df (pandas.DataFrame): DataFrame containing event information
            associations_df (pandas.DataFrame): DataFrame containing association information
            
        Returns:
            pandas.DataFrame: DataFrame containing combined company information
        """
        self.logger.info("Collecting company data from events and associations")
        
        # Scrape companies from events
        event_companies_df = self.scrape_companies_from_events(events_df)
        
        # Scrape companies from associations
        association_companies_df = self.scrape_companies_from_associations(associations_df)
        
        # Combine the two DataFrames
        if not event_companies_df.empty and not association_companies_df.empty:
            companies_df = pd.concat([event_companies_df, association_companies_df])
            companies_df = companies_df.drop_duplicates(subset=['name', 'website'])
        elif not event_companies_df.empty:
            companies_df = event_companies_df
        elif not association_companies_df.empty:
            companies_df = association_companies_df
        else:
            # If no companies found, create a sample dataset for testing
            self.logger.warning("No companies found from events or associations. Creating sample data.")
            companies_df = self._create_sample_companies()
        
        # Save the combined data
        companies_df.to_csv(self.output_dir / 'companies.csv', index=False)
        self.logger.info(f"Saved {len(companies_df)} companies to companies.csv")
        
        return companies_df
    
    def _create_sample_companies(self):
        """Create sample company data for testing
        
        Returns:
            pandas.DataFrame: DataFrame containing sample company information
        """
        sample_companies = [
            {
                'name': 'Acme Graphics Solutions',
                'website': 'https://www.acmegraphics.example.com',
                'description': 'Leading provider of graphics and signage solutions',
                'source_type': 'sample',
                'industry': 'Graphics & Signage',
                'relevance_score': 0.9
            },
            {
                'name': 'SignCraft Pro',
                'website': 'https://www.signcraftpro.example.com',
                'description': 'Professional sign manufacturing and installation',
                'source_type': 'sample',
                'industry': 'Graphics & Signage',
                'relevance_score': 0.85
            },
            {
                'name': 'VisualFX Displays',
                'website': 'https://www.visualfx.example.com',
                'description': 'Custom displays and visual merchandising solutions',
                'source_type': 'sample',
                'industry': 'Graphics & Signage',
                'relevance_score': 0.8
            },
            {
                'name': 'PrintMasters Inc.',
                'website': 'https://www.printmasters.example.com',
                'description': 'Commercial printing and large format graphics',
                'source_type': 'sample',
                'industry': 'Printing',
                'relevance_score': 0.75
            },
            {
                'name': 'BannerWorks',
                'website': 'https://www.bannerworks.example.com',
                'description': 'Specializing in banners and outdoor signage',
                'source_type': 'sample',
                'industry': 'Graphics & Signage',
                'relevance_score': 0.85
            }
        ]
        
        return pd.DataFrame(sample_companies)
    
    def scrape_companies_from_events(self, events_df):
        """Scrape company information from event websites
        
        Args:
            events_df (pandas.DataFrame): DataFrame containing event information
            
        Returns:
            pandas.DataFrame: DataFrame containing company information
        """
        self.logger.info(f"Scraping companies from {len(events_df)} events")
        
        all_companies = []
        
        # First, try to use ISA Expo Scraper to get real company data
        try:
            from src.data_collection.isa_expo_scraper import ISAExpoScraper
            self.logger.info("Using ISA Expo Scraper to get real company data")
            isa_scraper = ISAExpoScraper()
            isa_exhibitors_df = isa_scraper.scrape_exhibitors()
            
            if not isa_exhibitors_df.empty:
                self.logger.info(f"Successfully scraped {len(isa_exhibitors_df)} companies from ISA Expo")
                # Convert to the format expected by the rest of the pipeline
                for _, exhibitor in isa_exhibitors_df.iterrows():
                    company = {
                        'name': exhibitor.get('name', ''),
                        'website': exhibitor.get('website', ''),
                        'description': exhibitor.get('description', ''),
                        'source_type': 'event',
                        'source_event': 'ISA Sign Expo',
                        'industry': 'Graphics & Signage',
                        'relevance_score': 0.9
                    }
                    all_companies.append(company)
        except Exception as e:
            self.logger.error(f"Error using ISA Expo Scraper: {str(e)}")
        
        # If we didn't get any companies from ISA Expo, fall back to scraping from events
        if not all_companies:
            self.logger.info("Falling back to scraping companies from events")
            for _, event in events_df.iterrows():
                event_name = event['name']
                event_url = event['url']
                
                self.logger.info(f"Scraping companies from event: {event_name}")
                
                # Extract exhibitor list URL from event website
                exhibitor_url = self._get_exhibitor_list_url(event_url)
                
                if not exhibitor_url:
                    self.logger.warning(f"Could not find exhibitor list for {event_name}")
                    continue
                
                # Scrape companies from exhibitor list
                companies = self._scrape_exhibitor_list(exhibitor_url, event_name)
                
                if companies:
                    all_companies.extend(companies)
                    self.logger.info(f"Found {len(companies)} companies from {event_name}")
                else:
                    self.logger.warning(f"No companies found for {event_name}")
                
                # Respect rate limits
                time.sleep(self.delay)
        
        # Create DataFrame from all companies
        companies_df = pd.DataFrame(all_companies)
        
        # Save raw companies data
        if not companies_df.empty:
            companies_df.to_csv(self.output_dir / 'companies_raw.csv', index=False)
            self.logger.info(f"Saved {len(companies_df)} companies to companies_raw.csv")
        
        return companies_df
    
    def scrape_companies_from_associations(self, associations_df):
        """Scrape company information from industry association directories
        
        Args:
            associations_df (pandas.DataFrame): DataFrame containing association information
            
        Returns:
            pandas.DataFrame: DataFrame containing company information
        """
        self.logger.info(f"Scraping companies from {len(associations_df)} associations")
        
        all_companies = []
        
        for _, association in associations_df.iterrows():
            association_name = association['name']
            association_url = association['url']
            
            self.logger.info(f"Scraping companies from association: {association_name}")
            
            # Extract member directory URL from association website
            directory_url = self._get_member_directory_url(association_url)
            
            if not directory_url:
                self.logger.warning(f"Could not find member directory for {association_name}")
                continue
            
            # Scrape companies from member directory
            companies = self._scrape_member_directory(directory_url, association_name)
            
            if companies:
                all_companies.extend(companies)
                self.logger.info(f"Found {len(companies)} companies from {association_name}")
            else:
                self.logger.warning(f"No companies found for {association_name}")
            
            # Respect rate limits
            time.sleep(self.delay)
        
        # Create DataFrame from all companies
        companies_df = pd.DataFrame(all_companies)
        
        # Save raw companies data if not already saved
        output_file = self.output_dir / 'companies_raw.csv'
        if not output_file.exists() and not companies_df.empty:
            companies_df.to_csv(output_file, index=False)
            self.logger.info(f"Saved {len(companies_df)} companies to companies_raw.csv")
        elif not companies_df.empty:
            # Append to existing file
            existing_df = pd.read_csv(output_file)
            combined_df = pd.concat([existing_df, companies_df]).drop_duplicates(subset=['name', 'website'])
            combined_df.to_csv(output_file, index=False)
            self.logger.info(f"Updated companies_raw.csv with {len(companies_df)} new companies")
        
        return companies_df
    
    def _get_exhibitor_list_url(self, event_url):
        """Extract exhibitor list URL from event website
        
        Args:
            event_url (str): URL of the event website
            
        Returns:
            str: URL of the exhibitor list page
        """
        try:
            response = requests.get(event_url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Look for common patterns in links to exhibitor lists
            exhibitor_keywords = ['exhibitor', 'sponsor', 'vendor', 'directory']
            
            for keyword in exhibitor_keywords:
                # Find links containing the keyword
                links = soup.find_all('a', href=re.compile(keyword, re.IGNORECASE))
                
                if links:
                    # Get the first matching link
                    href = links[0].get('href')
                    
                    # Handle relative URLs
                    if href.startswith('/'):
                        base_url = '/'.join(event_url.split('/')[:3])  # http(s)://domain.com
                        href = base_url + href
                    elif not href.startswith(('http://', 'https://')):
                        href = event_url.rstrip('/') + '/' + href
                    
                    return href
            
            return None
        
        except Exception as e:
            self.logger.error(f"Error extracting exhibitor list URL from {event_url}: {str(e)}")
            return None
    
    def _scrape_exhibitor_list(self, exhibitor_url, event_name):
        """Scrape company information from exhibitor list page
        
        Args:
            exhibitor_url (str): URL of the exhibitor list page
            event_name (str): Name of the event
            
        Returns:
            list: List of dictionaries containing company information
        """
        try:
            response = requests.get(exhibitor_url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            companies = []
            
            # Look for common patterns in exhibitor listings
            # This is a simplified implementation and would need to be customized for each event website
            exhibitor_elements = soup.find_all(['div', 'li'], class_=re.compile('exhibitor|sponsor|vendor|company', re.IGNORECASE))
            
            if not exhibitor_elements:
                # Try alternative selectors
                exhibitor_elements = soup.find_all(['div', 'li'], id=re.compile('exhibitor|sponsor|vendor|company', re.IGNORECASE))
            
            if not exhibitor_elements:
                # Try finding tables
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    if len(rows) > 1:  # Assume first row is header
                        exhibitor_elements = rows[1:]  # Skip header row
                        break
            
            for element in exhibitor_elements:
                company = {}
                
                # Try to extract company name
                name_element = element.find(['h2', 'h3', 'h4', 'strong', 'b', 'a'])
                if name_element:
                    company['name'] = name_element.get_text().strip()
                else:
                    continue  # Skip if no name found
                
                # Try to extract company website
                website_element = element.find('a', href=re.compile('http|www'))
                if website_element:
                    company['website'] = website_element.get('href')
                else:
                    company['website'] = ''
                
                # Try to extract company description
                description_element = element.find(['p', 'div'], class_=re.compile('desc|about|info', re.IGNORECASE))
                if description_element:
                    company['description'] = description_element.get_text().strip()
                else:
                    company['description'] = ''
                
                # Add event information
                company['source_event'] = event_name
                company['source_type'] = 'event'
                
                companies.append(company)
            
            return companies
        
        except Exception as e:
            self.logger.error(f"Error scraping exhibitor list from {exhibitor_url}: {str(e)}")
            return []
    
    def _get_member_directory_url(self, association_url):
        """Extract member directory URL from association website
        
        Args:
            association_url (str): URL of the association website
            
        Returns:
            str: URL of the member directory page
        """
        try:
            response = requests.get(association_url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Look for common patterns in links to member directories
            directory_keywords = ['member', 'directory', 'companies', 'partners']
            
            for keyword in directory_keywords:
                # Find links containing the keyword
                links = soup.find_all('a', href=re.compile(keyword, re.IGNORECASE))
                
                if links:
                    # Get the first matching link
                    href = links[0].get('href')
                    
                    # Handle relative URLs
                    if href.startswith('/'):
                        base_url = '/'.join(association_url.split('/')[:3])  # http(s)://domain.com
                        href = base_url + href
                    elif not href.startswith(('http://', 'https://')):
                        href = association_url.rstrip('/') + '/' + href
                    
                    return href
            
            return None
        
        except Exception as e:
            self.logger.error(f"Error extracting member directory URL from {association_url}: {str(e)}")
            return None
    
    def _scrape_member_directory(self, directory_url, association_name):
        """Scrape company information from member directory page
        
        Args:
            directory_url (str): URL of the member directory page
            association_name (str): Name of the association
            
        Returns:
            list: List of dictionaries containing company information
        """
        try:
            response = requests.get(directory_url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            companies = []
            
            # Look for common patterns in member listings
            # This is a simplified implementation and would need to be customized for each association website
            member_elements = soup.find_all(['div', 'li'], class_=re.compile('member|company|partner', re.IGNORECASE))
            
            if not member_elements:
                # Try alternative selectors
                member_elements = soup.find_all(['div', 'li'], id=re.compile('member|company|partner', re.IGNORECASE))
            
            if not member_elements:
                # Try finding tables
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    if len(rows) > 1:  # Assume first row is header
                        member_elements = rows[1:]  # Skip header row
                        break
            
            for element in member_elements:
                company = {}
                
                # Try to extract company name
                name_element = element.find(['h2', 'h3', 'h4', 'strong', 'b', 'a'])
                if name_element:
                    company['name'] = name_element.get_text().strip()
                else:
                    continue  # Skip if no name found
                
                # Try to extract company website
                website_element = element.find('a', href=re.compile('http|www'))
                if website_element:
                    company['website'] = website_element.get('href')
                else:
                    company['website'] = ''
                
                # Try to extract company description
                description_element = element.find(['p', 'div'], class_=re.compile('desc|about|info', re.IGNORECASE))
                if description_element:
                    company['description'] = description_element.get_text().strip()
                else:
                    company['description'] = ''
                
                # Add association information
                company['source_association'] = association_name
                company['source_type'] = 'association'
                
                companies.append(company)
            
            return companies
        
        except Exception as e:
            self.logger.error(f"Error scraping member directory from {directory_url}: {str(e)}")
            return []