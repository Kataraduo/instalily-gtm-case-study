"""
Association data collection module for lead generation system.

This module is responsible for scraping information about industry associations
relevant to DuPont Tedlar's Graphics & Signage team.
"""

import logging
import time
import random
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
    REQUEST_DELAY,
    TARGET_INDUSTRY
)

class AssociationScraper:
    """Class for collecting data about industry associations"""
    
    def __init__(self):
        """Initialize association scraper"""
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
        
        # List of relevant industry associations for Graphics & Signage
        self.target_associations = [
            {
                'name': 'International Sign Association (ISA)',
                'url': 'https://www.signs.org/',
                'description': 'Trade association for sign industry professionals',
                'relevance_score': 0.95
            },
            {
                'name': 'Specialty Graphic Imaging Association (SGIA)',
                'url': 'https://www.sgia.org/',
                'description': 'Association for printing, graphics and visual communications',
                'relevance_score': 0.90
            },
            {
                'name': 'PRINTING United Alliance',
                'url': 'https://www.printingunited.com/',
                'description': 'Leading printing and graphic arts trade organization',
                'relevance_score': 0.85
            },
            {
                'name': 'Graphic Arts Association',
                'url': 'https://www.graphicartsassociation.org/',
                'description': 'Regional trade association for the printing and graphic communications industry',
                'relevance_score': 0.80
            },
            {
                'name': 'Society for Experiential Graphic Design (SEGD)',
                'url': 'https://segd.org/',
                'description': 'Multidisciplinary community creating experiences that connect people to place',
                'relevance_score': 0.85
            },
            {
                'name': 'Association for PRINT Technologies',
                'url': 'https://www.printtechnologies.org/',
                'description': 'Organization dedicated to supporting the entire commercial printing value chain',
                'relevance_score': 0.75
            },
            {
                'name': 'Flexographic Technical Association (FTA)',
                'url': 'https://www.flexography.org/',
                'description': 'Professional society dedicated to promoting and advancing the flexographic printing industry',
                'relevance_score': 0.70
            },
            {
                'name': 'American Advertising Federation (AAF)',
                'url': 'https://www.aaf.org/',
                'description': 'Advertising trade organization that includes members across all disciplines',
                'relevance_score': 0.65
            },
            {
                'name': 'United States Sign Council (USSC)',
                'url': 'https://www.ussc.org/',
                'description': 'Organization dedicated to serving the on-premise sign industry',
                'relevance_score': 0.90
            },
            {
                'name': 'National Association of Sign Supply Distributors (NASSD)',
                'url': 'https://www.nassd.org/',
                'description': 'Organization for sign supply distributors',
                'relevance_score': 0.85
            }
        ]
    
    def collect_associations_data(self) -> pd.DataFrame:
        """
        Collect data about industry associations
        
        Returns:
            pd.DataFrame: DataFrame containing association information
        """
        self.logger.info("Collecting data about industry associations")
        
        associations = []
        
        # First, add our predefined list of associations
        associations.extend(self.target_associations)
        
        # Then try to discover additional associations through web scraping
        try:
            additional_associations = self._discover_additional_associations()
            associations.extend(additional_associations)
        except Exception as e:
            self.logger.error(f"Error discovering additional associations: {str(e)}")
        
        # Create DataFrame
        associations_df = pd.DataFrame(associations)
        
        # Save to CSV
        output_file = self.output_dir / 'associations.csv'
        associations_df.to_csv(output_file, index=False)
        self.logger.info(f"Saved {len(associations_df)} associations to associations.csv")
        
        return associations_df
    
    def _discover_additional_associations(self) -> list:
        """
        Discover additional industry associations through web scraping
        
        Returns:
            list: List of additional association dictionaries
        """
        additional_associations = []
        
        # List of search queries to find additional associations
        search_queries = [
            "graphics signage industry associations",
            "sign making associations",
            "printing industry organizations",
            "visual communications trade groups",
            "digital printing associations"
        ]
        
        for query in search_queries:
            try:
                self.logger.info(f"Searching for associations with query: {query}")
                
                # Use a search engine API or direct scraping
                # For this example, we'll simulate finding associations
                # In a real implementation, you would use a search API or scrape search results
                
                # Simulate delay between requests
                time.sleep(random.uniform(self.delay, self.delay * 2))
                
                # For now, we'll return an empty list since we already have our predefined associations
                # In a real implementation, you would parse search results and extract association information
                
            except Exception as e:
                self.logger.error(f"Error searching for associations with query '{query}': {str(e)}")
        
        return additional_associations
    
    def _enrich_association_data(self, association: dict) -> dict:
        """
        Enrich association data with additional information from their website
        
        Args:
            association (dict): Association information dictionary
            
        Returns:
            dict: Enriched association dictionary
        """
        try:
            self.logger.info(f"Enriching data for association: {association['name']}")
            
            # Check if URL is available
            if not association.get('url'):
                self.logger.warning(f"No URL available for association: {association['name']}")
                return association
            
            # Fetch the association page
            response = requests.get(association['url'], headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract additional information
            
            # Look for member count
            member_text = soup.find(text=lambda t: t and any(x in t.lower() for x in ['member', 'members', 'membership']))
            if member_text:
                member_sentence = member_text.strip()
                association['member_info'] = member_sentence
            
            # Look for events
            events_section = soup.find(['h2', 'h3', 'h4', 'div'], text=lambda t: t and 'event' in t.lower())
            if events_section:
                events = []
                for event_elem in events_section.find_next_siblings(['div', 'ul', 'p']):
                    event_text = event_elem.get_text().strip()
                    if event_text:
                        events.append(event_text)
                
                if events:
                    association['events'] = events[:3]  # Limit to top 3 events
            
            # Add delay between requests
            time.sleep(self.delay)
            
        except Exception as e:
            self.logger.error(f"Error enriching data for association {association['name']}: {str(e)}")
        
        return association


def main():
    """Main function to run the association scraper"""
    scraper = AssociationScraper()
    associations_df = scraper.collect_associations_data()
    
    if not associations_df.empty:
        print(f"Successfully collected data for {len(associations_df)} associations")
        print(f"Data saved to {OUTPUT_DATA_DIR / 'associations.csv'}")
        
        # Display sample of the data
        print("\nSample of collected data:")
        print(associations_df[['name', 'url', 'relevance_score']].head())
    else:
        print("No association data was collected")


if __name__ == "__main__":
    main()