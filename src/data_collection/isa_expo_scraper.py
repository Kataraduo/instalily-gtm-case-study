"""ISA Sign Expo Scraper Module for Lead Generation System

This module is responsible for scraping exhibitor information from the ISA Sign Expo website,
specifically targeting the URL: https://isasignexpo2025.mapyourshow.com/
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


class ISAExpoScraper:
    """Class for scraping exhibitor information from ISA Sign Expo website"""
    
    def __init__(self):
        """Initialize the ISAExpoScraper with default headers and settings"""
        self.headers = {
            'User-Agent': USER_AGENT,
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.timeout = REQUEST_TIMEOUT
        self.delay = REQUEST_DELAY
        
        # Base URLs for the ISA Sign Expo website
        self.base_url = "https://isasignexpo2025.mapyourshow.com"
        self.exhibitor_list_url = f"{self.base_url}/8_0/#/searchtype/keyword/search/graphics/show/exhibitor"
        self.exhibitor_detail_base_url = f"{self.base_url}/8_0/exhibitor/exhibitor-details.cfm?exhid="
        
        # Ensure output directories exist
        self.output_dir = OUTPUT_DATA_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
    def scrape_exhibitors(self):
        """Scrape exhibitor information from ISA Sign Expo website
        
        Returns:
            pandas.DataFrame: DataFrame containing exhibitor information
        """
        self.logger.info("Starting to scrape exhibitors from ISA Sign Expo website")
        
        # Get all exhibitor links from the exhibitor list page
        exhibitor_links = self._get_exhibitor_links()
        
        if not exhibitor_links:
            self.logger.warning("No exhibitor links found")
            return pd.DataFrame()
        
        self.logger.info(f"Found {len(exhibitor_links)} exhibitor links")
        
        # Scrape details for each exhibitor
        all_exhibitors = []
        
        for exhid, name in exhibitor_links:
            try:
                self.logger.info(f"Scraping details for exhibitor: {name}")
                
                # Construct the exhibitor detail URL
                detail_url = f"{self.exhibitor_detail_base_url}{exhid}"
                
                # Scrape exhibitor details
                exhibitor_data = self._scrape_exhibitor_details(detail_url, name, exhid)
                
                if exhibitor_data:
                    all_exhibitors.append(exhibitor_data)
                    self.logger.info(f"Successfully scraped details for {name}")
                else:
                    self.logger.warning(f"Failed to scrape details for {name}")
                
                # Respect rate limits but use a minimal delay to speed up processing
                time.sleep(self.delay / 2)  # Use half the configured delay to speed up processing
                
            except Exception as e:
                self.logger.error(f"Error scraping exhibitor {name}: {str(e)}")
        
        # Create DataFrame from all exhibitors
        exhibitors_df = pd.DataFrame(all_exhibitors)
        
        # Save raw exhibitors data
        if not exhibitors_df.empty:
            output_file = self.output_dir / 'isa_expo_exhibitors.csv'
            exhibitors_df.to_csv(output_file, index=False)
            self.logger.info(f"Saved {len(exhibitors_df)} exhibitors to isa_expo_exhibitors.csv")
            
            # Also append to companies_raw.csv if it exists
            companies_file = self.output_dir / 'companies_raw.csv'
            if companies_file.exists():
                try:
                    companies_df = pd.read_csv(companies_file)
                    
                    # Rename columns to match companies_raw.csv format
                    exhibitors_df_renamed = exhibitors_df.rename(columns={
                        'name': 'name',
                        'website': 'website',
                        'description': 'description',
                        'booth': 'booth',
                        'address': 'address',
                        'city': 'city',
                        'state': 'state',
                        'zip': 'zip',
                        'country': 'country',
                        'phone': 'phone',
                        'product_categories': 'products'
                    })
                    
                    # Add source information
                    exhibitors_df_renamed['source_event'] = 'ISA Sign Expo 2025'
                    exhibitors_df_renamed['source_type'] = 'event'
                    
                    # Select only columns that exist in companies_raw.csv
                    common_columns = set(companies_df.columns).intersection(set(exhibitors_df_renamed.columns))
                    exhibitors_df_renamed = exhibitors_df_renamed[list(common_columns)]
                    
                    # Combine with existing companies
                    combined_df = pd.concat([companies_df, exhibitors_df_renamed])
                    combined_df = combined_df.drop_duplicates(subset=['name', 'website'])
                    combined_df.to_csv(companies_file, index=False)
                    self.logger.info(f"Updated companies_raw.csv with {len(exhibitors_df)} new companies")
                    
                except Exception as e:
                    self.logger.error(f"Error updating companies_raw.csv: {str(e)}")
        
        return exhibitors_df
    
    def _get_exhibitor_links(self):
        """Extract exhibitor links from the exhibitor list page
        
        Returns:
            list: List of tuples containing exhibitor IDs and names
        """
        try:
            self.logger.info(f"Fetching exhibitor list from {self.exhibitor_list_url}")
            
            response = requests.get(self.exhibitor_list_url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Find all exhibitor links on the page
            # Note: This selector might need to be adjusted based on the actual HTML structure
            exhibitor_links = []
            
            # Look for links to exhibitor detail pages
            links = soup.find_all('a', href=re.compile('exhibitor-details\.cfm\?exhid=\d+'))
            
            for link in links:
                href = link.get('href')
                name = link.get_text().strip()
                
                # Extract exhibitor ID from the URL
                exhid_match = re.search(r'exhid=(\d+)', href)
                if exhid_match:
                    exhid = exhid_match.group(1)
                    exhibitor_links.append((exhid, name))
            
            return exhibitor_links
            
        except Exception as e:
            self.logger.error(f"Error extracting exhibitor links: {str(e)}")
            return []
    
    def _scrape_exhibitor_details(self, detail_url, name, exhid):
        """Scrape detailed information for a single exhibitor
        
        Args:
            detail_url (str): URL of the exhibitor detail page
            name (str): Name of the exhibitor
            exhid (str): Exhibitor ID
            
        Returns:
            dict: Dictionary containing exhibitor information
        """
        try:
            response = requests.get(detail_url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Initialize exhibitor data with name and ID
            exhibitor = {
                'name': name,
                'exhid': exhid,
                'detail_url': detail_url
            }
            
            # Extract booth information
            booth_section = soup.find('h3', text=re.compile('Booths', re.IGNORECASE))
            if booth_section and booth_section.find_next('div'):
                exhibitor['booth'] = booth_section.find_next('div').get_text().strip()
            else:
                exhibitor['booth'] = ''
            
            # Extract company information
            company_section = soup.find('h3', text=re.compile('Company Information', re.IGNORECASE))
            if company_section:
                company_div = company_section.find_next('div')
                if company_div:
                    address_text = company_div.get_text().strip()
                    
                    # Parse address information
                    address_lines = [line.strip() for line in address_text.split('\n') if line.strip()]
                    
                    if len(address_lines) >= 1:
                        exhibitor['address'] = address_lines[0]
                    else:
                        exhibitor['address'] = ''
                    
                    # Try to extract city, state, zip from the second line
                    if len(address_lines) >= 2:
                        city_state_zip = address_lines[1]
                        city_state_match = re.match(r'([^,]+),?\s*([A-Z]{2})\s*(\d{5})?', city_state_zip)
                        
                        if city_state_match:
                            exhibitor['city'] = city_state_match.group(1).strip()
                            exhibitor['state'] = city_state_match.group(2).strip()
                            exhibitor['zip'] = city_state_match.group(3).strip() if city_state_match.group(3) else ''
                        else:
                            exhibitor['city'] = city_state_zip
                            exhibitor['state'] = ''
                            exhibitor['zip'] = ''
                    else:
                        exhibitor['city'] = ''
                        exhibitor['state'] = ''
                        exhibitor['zip'] = ''
                    
                    # Extract country from the third line
                    if len(address_lines) >= 3:
                        exhibitor['country'] = address_lines[2]
                    else:
                        exhibitor['country'] = ''
            else:
                exhibitor['address'] = ''
                exhibitor['city'] = ''
                exhibitor['state'] = ''
                exhibitor['zip'] = ''
                exhibitor['country'] = ''
            
            # Extract website
            website_link = soup.find('a', href=re.compile(r'^https?://'))
            if website_link:
                exhibitor['website'] = website_link.get('href')
            else:
                exhibitor['website'] = ''
            
            # Extract phone number
            phone_element = soup.find(text=re.compile(r'\d{3}-\d{3}-\d{4}'))
            if phone_element:
                exhibitor['phone'] = phone_element.strip()
            else:
                exhibitor['phone'] = ''
            
            # Extract company description/about
            about_section = soup.find('h3', text=re.compile('About', re.IGNORECASE))
            if about_section and about_section.find_next('div'):
                exhibitor['description'] = about_section.find_next('div').get_text().strip()
            else:
                exhibitor['description'] = ''
            
            # Extract product categories
            product_section = soup.find('h3', text=re.compile('Product Categories', re.IGNORECASE))
            if product_section:
                product_div = product_section.find_next('div')
                if product_div:
                    product_items = product_div.find_all('div', class_='item')
                    if product_items:
                        products = [item.get_text().strip() for item in product_items]
                        exhibitor['product_categories'] = '; '.join(products)
                    else:
                        exhibitor['product_categories'] = product_div.get_text().strip()
                else:
                    exhibitor['product_categories'] = ''
            else:
                exhibitor['product_categories'] = ''
            
            # Generate qualification reason based on collected data
            exhibitor['qualification_reason'] = self._generate_qualification_reason(exhibitor)
            
            return exhibitor
            
        except Exception as e:
            self.logger.error(f"Error scraping exhibitor details for {name}: {str(e)}")
            return None
    
    def _extract_revenue_and_size_info(self, exhibitor):
        """Extract revenue and company size information from exhibitor data
        
        Args:
            exhibitor (dict): Dictionary containing exhibitor information
            
        Returns:
            tuple: (revenue_info, size_info, revenue_score, size_score)
                revenue_info: String describing company revenue
                size_info: String describing company size
                revenue_score: Score based on revenue (0-20)
                size_score: Score based on company size (0-20)
        """
        description = str(exhibitor.get('description', '')).lower()
        website = str(exhibitor.get('website', '')).lower()
        
        # Combine description and website for analysis
        text = f"{description} {website}"
        
        # Initialize variables
        revenue_info = ""
        size_info = ""
        revenue_score = 0
        size_score = 0
        
        # Extract revenue information using regex patterns
        revenue_patterns = [
            # Billions pattern
            (r'\$(\d+(?:\.\d+)?)\s*b(?:illion)?', lambda x: float(x) * 1000000000, 20),  # $XB or $X billion
            (r'(\d+(?:\.\d+)?)\s*b(?:illion)?\s+(?:usd|\$|dollar)', lambda x: float(x) * 1000000000, 20),  # X billion USD/$/dollar
            
            # Millions pattern
            (r'\$(\d+(?:\.\d+)?)\s*m(?:illion)?', lambda x: float(x) * 1000000, 15),  # $XM or $X million
            (r'(\d+(?:\.\d+)?)\s*m(?:illion)?\s+(?:usd|\$|dollar)', lambda x: float(x) * 1000000, 15),  # X million USD/$/dollar
            
            # Revenue keywords with numbers
            (r'revenue\s+of\s+\$(\d+(?:\.\d+)?)\s*([bm](?:illion)?)', 
             lambda x, unit: float(x) * (1000000000 if unit.startswith('b') else 1000000), 18),
            (r'annual\s+revenue\s+(?:of\s+)?\$(\d+(?:\.\d+)?)\s*([bm](?:illion)?)', 
             lambda x, unit: float(x) * (1000000000 if unit.startswith('b') else 1000000), 18)
        ]
        
        # Extract employee/size information using regex patterns
        size_patterns = [
            # Specific employee counts
            (r'(\d+,\d+)\+?\s+employees', lambda x: int(x.replace(',', '')), 15),  # X,XXX employees
            (r'(\d+)\+?\s+employees', lambda x: int(x), 15),  # XXX employees
            (r'(?:over|more than)\s+(\d+,\d+)\s+employees', lambda x: int(x.replace(',', '')), 15),  # over X,XXX employees
            (r'(?:over|more than)\s+(\d+)\s+employees', lambda x: int(x), 15),  # over XXX employees
            
            # Size keywords
            (r'global\s+company', lambda: 5000, 18),  # global company
            (r'multinational\s+corporation', lambda: 10000, 20),  # multinational corporation
            (r'fortune\s+500', lambda: 10000, 20),  # Fortune 500
            (r'fortune\s+1000', lambda: 5000, 18),  # Fortune 1000
            (r'large\s+enterprise', lambda: 1000, 15),  # large enterprise
            (r'mid-sized|medium-sized', lambda: 250, 10),  # mid-sized
            (r'small\s+business', lambda: 50, 5)  # small business
        ]
        
        # Check for revenue information
        for pattern, value_func, score in revenue_patterns:
            matches = re.findall(pattern, text)
            if matches:
                if isinstance(matches[0], tuple):  # If the pattern has multiple capture groups
                    revenue_value = value_func(*matches[0])
                else:
                    revenue_value = value_func(matches[0])
                
                # Format revenue info
                if revenue_value >= 1000000000:  # Billions
                    revenue_info = f"${revenue_value/1000000000:.1f}B+ in revenue"
                    revenue_score = score
                elif revenue_value >= 1000000:  # Millions
                    revenue_info = f"${revenue_value/1000000:.1f}M+ in revenue"
                    revenue_score = score * 0.8  # Slightly lower score for millions
                break
        
        # Check for size information
        for pattern, value_func, score in size_patterns:
            matches = re.findall(pattern, text)
            if matches:
                if callable(value_func) and not matches[0]:  # For patterns without capture groups
                    employee_count = value_func()
                else:
                    employee_count = value_func(matches[0])
                
                # Format size info
                if employee_count >= 10000:
                    size_info = f"Large global company with {employee_count:,}+ employees"
                    size_score = score
                elif employee_count >= 1000:
                    size_info = f"Large company with {employee_count:,}+ employees"
                    size_score = score * 0.8
                elif employee_count >= 250:
                    size_info = f"Medium-sized company with {employee_count:,}+ employees"
                    size_score = score * 0.6
                else:
                    size_info = f"Company with {employee_count:,}+ employees"
                    size_score = score * 0.4
                break
        
        # If no specific information found, check for general size indicators
        if not size_info:
            if any(term in text for term in ['global', 'worldwide', 'international', 'multinational']):
                size_info = "Global company"
                size_score = 15
            elif any(term in text for term in ['national', 'leading', 'major']):
                size_info = "Major company"
                size_score = 10
            elif any(term in text for term in ['growing', 'expanding']):
                size_info = "Growing company"
                size_score = 5
        
        # If no specific revenue information found, check for general revenue indicators
        if not revenue_info:
            if any(term in text for term in ['billion', 'global leader', 'market leader']):
                revenue_info = "Significant revenue"
                revenue_score = 15
            elif any(term in text for term in ['million', 'profitable', 'successful']):
                revenue_info = "Established revenue"
                revenue_score = 10
        
        return revenue_info, size_info, revenue_score, size_score
    
    def _generate_qualification_reason(self, exhibitor):
        """Generate a qualification reason for why this exhibitor is a potential lead
        
        Args:
            exhibitor (dict): Dictionary containing exhibitor information
            
        Returns:
            str: Qualification reason
        """
        reasons = []
        score = 0  # Qualification score from 0-100
        
        # Check if they're in relevant product categories
        relevant_categories = {
            'vinyl': 25,  # High relevance
            'adhesive': 25,  # High relevance
            'graphic': 20,
            'sign': 15,
            'display': 15,
            'print': 15,
            'wrap': 20,
            'banner': 15,
            'film': 25,  # High relevance
            'laminate': 20,
            'consumable': 15,
            'material': 20,
            'substrate': 25,  # High relevance
            'surface': 15
        }
        
        if exhibitor.get('product_categories'):
            product_text = exhibitor['product_categories'].lower()
            matching_categories = []
            
            for cat, cat_score in relevant_categories.items():
                if cat in product_text:
                    matching_categories.append(cat)
                    # Add to score, but cap at 50 points for product categories
                    score = min(50, score + cat_score)
            
            if matching_categories:
                reasons.append(f"Offers products in relevant categories: {', '.join(matching_categories)}")
        
        # Check company description for relevant keywords
        relevant_description_terms = {
            'vinyl': 10,
            'film': 10,
            'material': 8,
            'substrate': 10,
            'surface': 8,
            'adhesive': 10,
            'graphic': 8,
            'signage': 5,
            'printing': 5,
            'quality': 3,
            'durable': 5,
            'weather': 5,
            'outdoor': 5,
            'indoor': 3,
            'application': 3,
            'installation': 3
        }
        
        if exhibitor.get('description'):
            desc_text = exhibitor['description'].lower()
            matching_terms = []
            
            for term, term_score in relevant_description_terms.items():
                if term in desc_text:
                    matching_terms.append(term)
                    # Add to score, but cap at 30 points for description terms
                    score = min(30, score + term_score)
            
            if matching_terms:
                reasons.append(f"Company description mentions relevant terms: {', '.join(matching_terms)}")
        
        # Extract and score company size and revenue information
        revenue_info, size_info, revenue_score, size_score = self._extract_revenue_and_size_info(exhibitor)
        
        # Add revenue and size information to reasons if available
        if revenue_info:
            reasons.append(revenue_info)
            score += revenue_score  # Add revenue score to total score
        
        if size_info:
            reasons.append(size_info)
            score += size_score  # Add size score to total score
        
        # Check if they have a booth (indicating they're a confirmed exhibitor)
        if exhibitor.get('booth') and exhibitor['booth'].strip():
            reasons.append(f"Confirmed exhibitor with booth {exhibitor['booth']}")
            score += 10  # Add points for having a confirmed booth
        
        # Check if they have a website (for further research)
        if exhibitor.get('website') and exhibitor['website'].strip():
            reasons.append("Has website for further qualification")
            score += 5  # Add points for having a website
        
        # Check if they have contact information
        if exhibitor.get('phone') and exhibitor['phone'].strip():
            score += 5  # Add points for having contact information
        
        # Generate qualification tier based on score
        if score >= 80:  # Increased threshold for Tier 1 due to additional scoring factors
            tier = "Tier 1 (High Priority)"
        elif score >= 50:  # Increased threshold for Tier 2 due to additional scoring factors
            tier = "Tier 2 (Medium Priority)"
        else:
            tier = "Tier 3 (Low Priority)"
        
        # Add qualification tier to reasons
        reasons.append(f"Qualification Score: {score}/100 - {tier}")
        
        # If we have no specific reasons but they're an exhibitor, provide a generic reason
        if len(reasons) <= 1:  # Only has the tier reason
            reasons.append("Exhibitor at ISA Sign Expo 2025 (graphics industry event)")
        
        return " | ".join(reasons)


    def parse_exhibitor_text(self, text_data):
        """Parse exhibitor information from provided text data
        
        This method is used as a fallback when web scraping doesn't work. It parses
        exhibitor information from text data copied from the ISA Sign Expo website.
        
        Args:
            text_data (str): Text data containing exhibitor information
            
        Returns:
            pandas.DataFrame: DataFrame containing exhibitor information
        """
        self.logger.info("Parsing exhibitor information from provided text data")
        
        all_exhibitors = []
        
        # Split the text into sections (Featured Exhibitors and All Exhibitors)
        sections = text_data.split("All Exhibitors")
        
        # Process both sections
        for section_idx, section in enumerate(sections):
            # Skip empty sections
            if not section.strip():
                continue
                
            # Determine if this is the featured section
            is_featured = section_idx == 0 and "Featured Exhibitors" in section
            
            # Split the section into exhibitor blocks
            # Each exhibitor block starts with a company name and ends before the next company name
            exhibitor_blocks = re.split(r'\n(?=[A-Z][\w\s]+\n)', section)
            
            for block in exhibitor_blocks:
                # Skip headers and empty blocks
                if not block.strip() or any(header in block for header in ["ExhibitorSummaryBoothAdd to Planner", "Featured Exhibitors", "See Results on Floor Plan"]):
                    continue
                
                # Extract company name (first line)
                lines = block.strip().split('\n')
                if not lines:
                    continue
                    
                company_name = lines[0].strip()
                
                # Skip if this is not a company entry
                if not company_name or company_name in ["Grid List", "See Results on Floor Plan"]:
                    continue
                
                # Initialize exhibitor data
                exhibitor = {
                    'name': company_name,
                    'exhid': '',  # No ID in text data
                    'detail_url': '',  # No URL in text data
                    'featured': is_featured
                }
                
                # Extract description (all lines except first and last)
                if len(lines) > 2:
                    description = ' '.join(lines[1:-1]).strip()
                    # Clean up description (remove ellipsis if present)
                    if description.endswith('...'):
                        description = description[:-3].strip()
                    exhibitor['description'] = description
                else:
                    exhibitor['description'] = ''
                
                # Extract booth number (last line)
                if len(lines) > 1:
                    booth = lines[-1].strip()
                    # Check if it's a valid booth number (typically numeric or alphanumeric)
                    if re.match(r'^[\w\d]+$', booth):
                        exhibitor['booth'] = booth
                    else:
                        exhibitor['booth'] = ''
                else:
                    exhibitor['booth'] = ''
                
                # Add placeholder values for fields not available in text data
                exhibitor['website'] = ''
                exhibitor['address'] = ''
                exhibitor['city'] = ''
                exhibitor['state'] = ''
                exhibitor['zip'] = ''
                exhibitor['country'] = ''
                exhibitor['phone'] = ''
                exhibitor['product_categories'] = 'Graphics'  # Since these are from a graphics search
                
                # Generate qualification reason
                exhibitor['qualification_reason'] = self._generate_qualification_reason(exhibitor)
                
                all_exhibitors.append(exhibitor)
        
        # Create DataFrame from all exhibitors
        exhibitors_df = pd.DataFrame(all_exhibitors)
        
        # Save raw exhibitors data
        if not exhibitors_df.empty:
            output_file = self.output_dir / 'isa_expo_exhibitors_from_text.csv'
            exhibitors_df.to_csv(output_file, index=False)
            self.logger.info(f"Saved {len(exhibitors_df)} exhibitors from text data to isa_expo_exhibitors_from_text.csv")
            
            # Also append to companies_raw.csv if it exists
            companies_file = self.output_dir / 'companies_raw.csv'
            if companies_file.exists():
                try:
                    companies_df = pd.read_csv(companies_file)
                    
                    # Rename columns to match companies_raw.csv format
                    exhibitors_df_renamed = exhibitors_df.rename(columns={
                        'name': 'name',
                        'website': 'website',
                        'description': 'description',
                        'booth': 'booth',
                        'address': 'address',
                        'city': 'city',
                        'state': 'state',
                        'zip': 'zip',
                        'country': 'country',
                        'phone': 'phone',
                        'product_categories': 'products'
                    })
                    
                    # Add source information
                    exhibitors_df_renamed['source_event'] = 'ISA Sign Expo 2025'
                    exhibitors_df_renamed['source_type'] = 'event'
                    
                    # Select only columns that exist in companies_raw.csv
                    common_columns = set(companies_df.columns).intersection(set(exhibitors_df_renamed.columns))
                    exhibitors_df_renamed = exhibitors_df_renamed[list(common_columns)]
                    
                    # Combine with existing companies
                    combined_df = pd.concat([companies_df, exhibitors_df_renamed])
                    combined_df = combined_df.drop_duplicates(subset=['name', 'website'])
                    combined_df.to_csv(companies_file, index=False)
                    self.logger.info(f"Updated companies_raw.csv with {len(exhibitors_df)} new companies from text data")
                    
                except Exception as e:
                    self.logger.error(f"Error updating companies_raw.csv with text data: {str(e)}")
        
        return exhibitors_df


def main():
    """Main function to run the ISA Expo scraper"""
    scraper = ISAExpoScraper()
    
    # Try web scraping first
    exhibitors_df = scraper.scrape_exhibitors()
    
    if not exhibitors_df.empty:
        print(f"Successfully scraped {len(exhibitors_df)} exhibitors from ISA Sign Expo website")
        print(f"Data saved to {OUTPUT_DATA_DIR / 'isa_expo_exhibitors.csv'}")
        
        # Display sample of the data
        print("\nSample of scraped data:")
        print(exhibitors_df[['name', 'booth', 'product_categories', 'qualification_reason']].head())
    else:
        print("Web scraping failed. No exhibitors were scraped from the ISA Sign Expo website")
        print("You can use the parse_exhibitor_text method to parse exhibitor information from text data")


if __name__ == "__main__":
    main()