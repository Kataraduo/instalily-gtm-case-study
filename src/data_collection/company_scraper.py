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
        self.logger.info("Collecting company data from ISA Sign Expo 2025")
        
        # Get real company data from ISA Sign Expo 2025
        from src.data_collection.isa_expo_companies import ISAExpoCompanies
        isa_companies = ISAExpoCompanies()
        companies_df = isa_companies.get_companies()
        self.logger.info(f"Using {len(companies_df)} real companies from ISA Sign Expo 2025")
        
        # Save the real company data to CSV
        companies_df.to_csv(self.output_dir / 'companies.csv', index=False)
        self.logger.info(f"Saved {len(companies_df)} real companies from ISA Sign Expo 2025 to companies.csv")
        
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
        
    def parse_companies_from_text(self, text_data):
        """Parse company information from provided text data
        
        This method is used when web scraping doesn't work. It parses company information
        from text data copied from websites like ISA Sign Expo 2025.
        
        Args:
            text_data (str): Text data containing company information
            
        Returns:
            pandas.DataFrame: DataFrame containing company information
        """
        self.logger.info("Parsing company information from provided text data")
        
        all_companies = []
        
        # Split the text into sections (Featured Exhibitors and All Exhibitors)
        sections = text_data.split("All Exhibitors")
        
        # Process both sections
        for section_idx, section in enumerate(sections):
            # Skip empty sections
            if not section.strip():
                continue
                
            # Determine if this is the featured section
            is_featured = section_idx == 0 and "Featured Exhibitors" in section
            
            # Split the section into company blocks
            # Each company block starts with a company name and ends before the next company name
            company_blocks = re.split(r'\n(?=[A-Z][\w\s]+\n)', section)
            
            for block in company_blocks:
                # Skip headers and empty blocks
                if not block.strip() or any(header in block for header in ["ExhibitorSummaryBoothAdd to Planner", "Featured Exhibitors", "See Results on Floor Plan"]):
                    continue
                
                # Extract company name (first line)
                lines = block.strip().split('\n')
                if not lines:
                    continue
                    
                company_name = lines[0].strip()
                
                # Skip if this is not a company entry
                if not company_name or company_name in ["Grid List", "See Results on Floor Plan", "Results for Keyword"]:
                    continue
                
                # Initialize company data
                company = {
                    'name': company_name,
                    'featured': is_featured
                }
                
                # Extract description (all lines except first and last)
                if len(lines) > 2:
                    description = ' '.join(lines[1:-1]).strip()
                    # Clean up description (remove ellipsis if present)
                    if description.endswith('...'):
                        description = description[:-3].strip()
                    company['description'] = description
                else:
                    company['description'] = ''
                
                # Extract booth number (last line)
                if len(lines) > 1:
                    booth = lines[-1].strip()
                    # Check if it's a valid booth number (typically numeric or alphanumeric)
                    if re.match(r'^[\w\d, ]+$', booth):
                        company['booth'] = booth
                    else:
                        company['booth'] = ''
                else:
                    company['booth'] = ''
                
                # Add source information
                company['source_type'] = 'event'
                company['source_event'] = 'ISA Sign Expo 2025'
                company['industry'] = 'Graphics & Signage'
                company['relevance_score'] = 0.9 if is_featured else 0.8
                
                # Add placeholder for website (not available in text data)
                company['website'] = ''
                
                all_companies.append(company)
        
        # Create DataFrame from all companies
        companies_df = pd.DataFrame(all_companies)
        
        # Save raw companies data
        if not companies_df.empty:
            output_file = self.output_dir / 'companies_from_text.csv'
            companies_df.to_csv(output_file, index=False)
            self.logger.info(f"Saved {len(companies_df)} companies from text data to companies_from_text.csv")
            
            # Also append to companies_raw.csv if it exists
            companies_file = self.output_dir / 'companies_raw.csv'
            if companies_file.exists():
                try:
                    existing_df = pd.read_csv(companies_file)
                    combined_df = pd.concat([existing_df, companies_df])
                    combined_df = combined_df.drop_duplicates(subset=['name'])
                    combined_df.to_csv(companies_file, index=False)
                    self.logger.info(f"Updated companies_raw.csv with {len(companies_df)} new companies from text data")
                except Exception as e:
                    self.logger.error(f"Error updating companies_raw.csv with text data: {str(e)}")
        
        return companies_df
    
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
            
            # Try web scraping first
            self.logger.info("Attempting to scrape companies from ISA Sign Expo website")
            isa_exhibitors_df = isa_scraper.scrape_exhibitors()
            
            if not isa_exhibitors_df.empty:
                self.logger.info(f"Successfully scraped {len(isa_exhibitors_df)} companies from ISA Expo")
                # Convert to the format expected by the rest of the pipeline
                for _, exhibitor in isa_exhibitors_df.iterrows():
                    company = {
                        'name': exhibitor.get('name', ''),
                        'website': exhibitor.get('website', ''),
                        'description': exhibitor.get('description', ''),
                        'booth': exhibitor.get('booth', ''),
                        'source_type': 'event',
                        'source_event': 'ISA Sign Expo',
                        'industry': 'Graphics & Signage',
                        'relevance_score': 0.9
                    }
                    all_companies.append(company)
            else:
                # If web scraping fails, check if we have text data to parse
                self.logger.info("Web scraping failed. Checking for text data to parse...")
                
                # Sample text data from ISA Sign Expo 2025 with graphics keyword search
                # This would typically come from user input or a file
                isa_expo_text = """
                100 Results for Keyword: "graphics"
                 Grid List
                Featured Exhibitors ( 5 )
                 See Results on Floor Plan
                ExhibitorSummaryBoothAdd to Planner
                CUTWORX USA
                CUTWORX USA offers a complete line of finishing solutions for all your printing, cutting, laminating, and textile needs. CUTWORX USA was founded to consolidate our focus on digital finishing equipm...
                2637
                General Formulations
                General Formulations® (GF) is a global manufacturer of pressure-sensitive print media headquartered in the USA, since 1953. GF offers a cross-platform portfolio of print and cut film and laminate solu...
                1937
                Laguna Tools Inc.
                For over four decades, Laguna Tools has been a pioneer in the machinery industry, delivering innovative solutions that empower artisans, craftsmen, and businesses to achieve unparalleled precision and...
                1021
                 
                Lintec of America, Inc.
                Lintec Corporation is a premier supplier of pressure sensitive films and specialty media. Please visit our booth to see our lineup of optically clear window graphics films WINCOS TM.
                2364
                Signage Details
                Subscribe today for unlimited access to proven, industry-standard, permit-ready section details for fabricating and installing commercial signs. With our exclusive, patent-pending, intuitive Select-A-...
                3813
                All Exhibitors ( 100 )
                 See Results on Floor Plan
                ExhibitorSummaryBoothAdd to Planner
                 
                3A Composites USA, Inc.
                3A Composites USA specializes in the manufacturing of leading composite substrates for the display, graphic arts, signage & framing industries throughout the Americas. Category defining brands like DI...
                1222
                3M Commercial Solutions
                3M Commercial Graphics helps customers worldwide build brands by providing total large-format graphics and light management solutions. 3M manufactures or certifies lighting solutions, graphic films an...
                4725
                 
                A.R.K. Ramos Foundry & Mfg. Co.
                A.R.K. Ramos manufactures cast and etched aluminum, brass, and bronze plaques. We also produce cast letters, cut graphics, and reverse channel letters in a variety of metals including aluminum, brass,...
                4549
                 
                Abitech
                Abitech is a distinguished wholesale distributor specializing in signage materials and graphics. Our expertise lies in delivering the utmost quality materials at competitive prices with a dedicated fo...
                4618
                 
                ADMAX Exhibit & Display Ltd.
                Established in 1999, Admax Exhibit & Display Ltd. is one of the biggest display company in China. Admax is specialized in supplying modular exhibits, portable display, creative signs and custom printi...
                2369, 4018
                Advanced Greig Laminators, Inc.
                AGL is the leading manufacturer/distributor of high quality laminating equipment and finishing supplies. At ISA 2025, AGL will showcase two examples of our class leading laminators. Each are designed ...
                4749
                """
                
                # Parse the text data
                text_exhibitors_df = isa_scraper.parse_exhibitor_text(isa_expo_text)
                
                if not text_exhibitors_df.empty:
                    self.logger.info(f"Successfully parsed {len(text_exhibitors_df)} companies from ISA Expo text data")
                    # Convert to the format expected by the rest of the pipeline
                    for _, exhibitor in text_exhibitors_df.iterrows():
                        company = {
                            'name': exhibitor.get('name', ''),
                            'website': exhibitor.get('website', ''),
                            'description': exhibitor.get('description', ''),
                            'booth': exhibitor.get('booth', ''),
                            'source_type': 'event',
                            'source_event': 'ISA Sign Expo',
                            'industry': 'Graphics & Signage',
                            'relevance_score': 0.85  # Slightly lower score for text-parsed data
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