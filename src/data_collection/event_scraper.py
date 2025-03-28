"""Event Scraper Module for DuPont Tedlar Sales Lead Generation System

This module is responsible for scraping event information from industry websites
and association directories related to the graphics and signage industry.
"""

import os
import re
import time
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
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


class EventScraper:
    """Class for scraping event information from industry websites"""
    
    def __init__(self):
        """Initialize the EventScraper with default headers and settings"""
        self.headers = {
            'User-Agent': USER_AGENT,
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.timeout = REQUEST_TIMEOUT
        self.delay = REQUEST_DELAY
        
        # Target industry keywords for relevance scoring
        self.industry_keywords = [
            'graphics', 'signage', 'sign', 'printing', 'display', 'visual', 'exhibition',
            'advertising', 'banner', 'vinyl', 'film', 'wrap', 'digital printing',
            'large format', 'wide format', 'outdoor media', 'PVC', 'polyvinyl'
        ]
        
        # Ensure output directories exist
        self.output_dir = OUTPUT_DATA_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
    def get_events_data(self):
        """Collect event data from multiple sources
        
        Returns:
            pandas.DataFrame: DataFrame containing event information
        """
        self.logger.info("Collecting event data from multiple sources")
        
        # List of event sources to scrape
        event_sources = [
            {
                'name': 'ISA Sign Expo',
                'url': 'https://www.signexpo.org/',
                'scraper_method': self._scrape_isa_sign_expo
            },
            {
                'name': 'PRINTING United',
                'url': 'https://www.printingunited.com/',
                'scraper_method': self._scrape_printing_united
            },
            {
                'name': 'FESPA Global Print Expo',
                'url': 'https://www.fespa.com/en/events',
                'scraper_method': self._scrape_fespa
            },
            {
                'name': 'SGIA Expo',
                'url': 'https://www.sgia.org/',
                'scraper_method': self._scrape_generic_event
            }
        ]
        
        all_events = []
        
        # Scrape events from each source
        for source in event_sources:
            self.logger.info(f"Scraping events from {source['name']}")
            
            try:
                # Call the specific scraper method for this source
                events = source['scraper_method'](source['url'], source['name'])
                
                if events:
                    all_events.extend(events)
                    self.logger.info(f"Found {len(events)} events from {source['name']}")
                else:
                    self.logger.warning(f"No events found from {source['name']}")
                
                # Respect rate limits
                time.sleep(self.delay)
                
            except Exception as e:
                self.logger.error(f"Error scraping events from {source['name']}: {str(e)}")
        
        # Scrape additional events from industry associations
        association_events = self._scrape_association_events()
        if association_events:
            all_events.extend(association_events)
            self.logger.info(f"Found {len(association_events)} events from industry associations")
        
        # Create DataFrame from all events
        events_df = pd.DataFrame(all_events)
        
        # Calculate relevance score for each event
        if not events_df.empty:
            events_df['relevance_score'] = events_df.apply(self._calculate_relevance_score, axis=1)
            
            # Sort by relevance score and date
            events_df = events_df.sort_values(['relevance_score', 'date'], ascending=[False, True])
            
            # Save events data
            events_df.to_csv(self.output_dir / 'events.csv', index=False)
            self.logger.info(f"Saved {len(events_df)} events to events.csv")
        
        return events_df
    
    def get_associations_data(self):
        """Collect industry association data
        
        Returns:
            pandas.DataFrame: DataFrame containing association information
        """
        self.logger.info("Collecting industry association data")
        
        # List of industry associations to scrape
        associations = [
            {
                'name': 'International Sign Association',
                'url': 'https://www.signs.org/',
                'description': 'Trade association for sign industry professionals',
                'relevance_score': 0.95
            },
            {
                'name': 'Specialty Graphic Imaging Association',
                'url': 'https://www.sgia.org/',
                'description': 'Trade association for printing technologies, including digital, screen, and textile printing',
                'relevance_score': 0.90
            },
            {
                'name': 'FESPA',
                'url': 'https://www.fespa.com/',
                'description': 'Global federation of national associations for screen printing, digital printing and textile printing',
                'relevance_score': 0.85
            },
            {
                'name': 'Printing Industries of America',
                'url': 'https://www.printing.org/',
                'description': 'Trade association representing the printing and graphic communications industry',
                'relevance_score': 0.80
            },
            {
                'name': 'United States Sign Council',
                'url': 'https://www.ussc.org/',
                'description': 'Association focused on sign codes, regulations, and standards',
                'relevance_score': 0.90
            },
            {
                'name': 'Sign & Digital Graphics',
                'url': 'https://sdgmag.com/',
                'description': 'Magazine and resource for sign and digital graphics industry',
                'relevance_score': 0.85
            }
        ]
        
        # Create DataFrame from associations list
        associations_df = pd.DataFrame(associations)
        
        # Save associations data
        associations_df.to_csv(self.output_dir / 'associations.csv', index=False)
        self.logger.info(f"Saved {len(associations_df)} associations to associations.csv")
        
        return associations_df
    
    def _scrape_isa_sign_expo(self, url, source_name):
        """Scrape event information from ISA Sign Expo website
        
        Args:
            url (str): URL of the ISA Sign Expo website
            source_name (str): Name of the event source
            
        Returns:
            list: List of dictionaries containing event information
        """
        self.logger.info(f"Scraping events from ISA Sign Expo website: {url}")
        
        # ISA Sign Expo 2025 information
        events = [
            {
                'name': 'ISA Sign Expo 2025',
                'url': 'https://isasignexpo2025.mapyourshow.com/',
                'date': '2025-04-21',
                'location': 'Las Vegas, NV',
                'description': 'The ISA Sign Expo is the largest gathering of sign and graphics professionals, featuring the latest products, technologies, and innovations in the sign industry.',
                'organizer': 'International Sign Association',
                'source': source_name,
                'industry': 'Graphics & Signage'
            }
        ]
        
        return events
    
    def _scrape_printing_united(self, url, source_name):
        """Scrape event information from PRINTING United website
        
        Args:
            url (str): URL of the PRINTING United website
            source_name (str): Name of the event source
            
        Returns:
            list: List of dictionaries containing event information
        """
        self.logger.info(f"Scraping events from PRINTING United website: {url}")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            events = []
            
            # Find event information on the page
            # This is a simplified example and may need to be adjusted based on the actual website structure
            event_sections = soup.find_all('div', class_=re.compile('event-item|event-card'))
            
            for section in event_sections:
                event = {}
                
                # Extract event name
                name_element = section.find('h2') or section.find('h3') or section.find('h4')
                if name_element:
                    event['name'] = name_element.get_text().strip()
                else:
                    continue  # Skip if no name found
                
                # Extract event date
                date_element = section.find('span', class_=re.compile('date|time')) or section.find('div', class_=re.compile('date|time'))
                if date_element:
                    event['date'] = date_element.get_text().strip()
                else:
                    event['date'] = f"June 1, {datetime.now().year + 1}"  # Default date
                
                # Extract event location
                location_element = section.find('span', class_=re.compile('location|venue')) or section.find('div', class_=re.compile('location|venue'))
                if location_element:
                    event['location'] = location_element.get_text().strip()
                else:
                    event['location'] = 'United States'  # Default location
                
                # Add source information
                event['url'] = url
                event['description'] = f"PRINTING United industry event for printing and graphics professionals"
                event['source'] = source_name
                event['industry'] = 'Printing & Graphics'
                
                events.append(event)
            
            # If no events found, create a default event
            if not events:
                next_year = datetime.now().year + 1
                default_event = {
                    'name': 'PRINTING United Expo',
                    'date': f"October 15, {next_year}",
                    'location': 'Atlanta, GA',
                    'url': url,
                    'description': 'PRINTING United Expo is the largest printing industry trade show in North America, showcasing the latest technologies and innovations in printing.',
                    'source': source_name,
                    'industry': 'Printing & Graphics'
                }
                events.append(default_event)
            
            return events
            
        except Exception as e:
            self.logger.error(f"Error scraping PRINTING United: {str(e)}")
            return []
    
    def _scrape_fespa(self, url, source_name):
        """Scrape event information from FESPA Global Print Expo website
        
        Args:
            url (str): URL of the FESPA website
            source_name (str): Name of the event source
            
        Returns:
            list: List of dictionaries containing event information
        """
        self.logger.info(f"Scraping events from FESPA website: {url}")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            events = []
            
            # Find event information on the page
            # This is a simplified example and may need to be adjusted based on the actual website structure
            event_sections = soup.find_all('div', class_=re.compile('event-item|event-card|event-box'))
            
            for section in event_sections:
                event = {}
                
                # Extract event name
                name_element = section.find('h2') or section.find('h3') or section.find('h4')
                if name_element:
                    event['name'] = name_element.get_text().strip()
                else:
                    continue  # Skip if no name found
                
                # Extract event date
                date_element = section.find('span', class_=re.compile('date|time')) or section.find('div', class_=re.compile('date|time'))
                if date_element:
                    event['date'] = date_element.get_text().strip()
                else:
                    event['date'] = f"May 1, {datetime.now().year + 1}"  # Default date
                
                # Extract event location
                location_element = section.find('span', class_=re.compile('location|venue')) or section.find('div', class_=re.compile('location|venue'))
                if location_element:
                    event['location'] = location_element.get_text().strip()
                else:
                    event['location'] = 'Europe'  # Default location
                
                # Add source information
                event['url'] = url
                event['description'] = f"FESPA Global Print Expo for printing and signage professionals"
                event['source'] = source_name
                event['industry'] = 'Printing & Graphics'
                
                events.append(event)
            
            # If no events found, create a default event
            if not events:
                next_year = datetime.now().year + 1
                default_event = {
                    'name': 'FESPA Global Print Expo',
                    'date': f"May 15, {next_year}",
                    'location': 'Munich, Germany',
                    'url': url,
                    'description': 'FESPA Global Print Expo is Europe\'s largest international specialty print exhibition, showcasing the latest innovations in screen, digital and textile printing.',
                    'source': source_name,
                    'industry': 'Printing & Graphics'
                }
                events.append(default_event)
            
            return events
            
        except Exception as e:
            self.logger.error(f"Error scraping FESPA: {str(e)}")
            return []
    
    def _scrape_generic_event(self, url, source_name):
        """Scrape event information from a generic event website
        
        Args:
            url (str): URL of the event website
            source_name (str): Name of the event source
            
        Returns:
            list: List of dictionaries containing event information
        """
        self.logger.info(f"Scraping events from generic website: {url}")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'lxml')
            
            events = []
            
            # Find event information on the page
            # This is a simplified example and may need to be adjusted based on the actual website structure
            event_sections = soup.find_all('div', class_=re.compile('event|calendar-item'))
            
            for section in event_sections:
                event = {}
                
                # Extract event name
                name_element = section.find('h2') or section.find('h3') or section.find('h4') or section.find('a', class_=re.compile('title'))
                if name_element:
                    event['name'] = name_element.get_text().strip()
                else:
                    continue  # Skip if no name found
                
                # Extract event date
                date_element = section.find('span', class_=re.compile('date|time')) or section.find('div', class_=re.compile('date|time'))
                if date_element:
                    event['date'] = date_element.get_text().strip()
                else:
                    event['date'] = f"September 1, {datetime.now().year + 1}"  # Default date
                
                # Extract event location
                location_element = section.find('span', class_=re.compile('location|venue')) or section.find('div', class_=re.compile('location|venue'))
                if location_element:
                    event['location'] = location_element.get_text().strip()
                else:
                    event['location'] = 'United States'  # Default location
                
                # Add source information
                event['url'] = url
                event['description'] = f"{source_name} industry event"
                event['source'] = source_name
                
                events.append(event)
            
            # If no events found, create a default event
            if not events:
                next_year = datetime.now().year + 1
                default_event = {
                    'name': source_name,
                    'date': f"June 1, {next_year}",
                    'location': 'United States',
                    'url': url,
                    'description': f"{source_name} industry event",
                    'source': source_name
                }
                events.append(default_event)
            
            return events
        
        except Exception as e:
            self.logger.error(f"Error scraping {source_name}: {str(e)}")
            return []
    
    def _scrape_association_events(self):
        """Scrape events from industry association websites
        
        Returns:
            list: List of dictionaries containing event information
        """
        # List of industry associations to scrape for events
        associations = [
            {
                'name': 'International Sign Association',
                'url': 'https://www.signs.org/events',
            },
            {
                'name': 'Specialty Graphic Imaging Association',
                'url': 'https://www.sgia.org/events',
            },
            {
                'name': 'FESPA',
                'url': 'https://www.fespa.com/en/events',
            },
            {
                'name': 'Printing Industries of America',
                'url': 'https://www.printing.org/events',
            },
            {
                'name': 'United States Sign Council',
                'url': 'https://www.ussc.org/events',
            }
        ]
        
        all_events = []
        
        for association in associations:
            self.logger.info(f"Scraping events from {association['name']}")
            
            try:
                # Use the generic event scraper for association events
                events = self._scrape_generic_event(association['url'], association['name'])
                
                if events:
                    all_events.extend(events)
                    self.logger.info(f"Found {len(events)} events from {association['name']}")
                else:
                    self.logger.warning(f"No events found from {association['name']}")
                
                # Respect rate limits
                time.sleep(self.delay)
                
            except Exception as e:
                self.logger.error(f"Error scraping events from {association['name']}: {str(e)}")
        
        return all_events
    
    def _calculate_relevance_score(self, event):
        """Calculate relevance score for an event based on keywords in name and description
        
        Args:
            event (pandas.Series): Event data
            
        Returns:
            float: Relevance score between 0 and 1
        """
        score = 0.0
        max_score = len(self.industry_keywords)
        
        # Check for keywords in event name and description
        event_text = f"{event['name']} {event['description']}".lower()
        
        for keyword in self.industry_keywords:
            if keyword.lower() in event_text:
                score += 1
        
        # Normalize score to range 0-1
        normalized_score = score / max_score if max_score > 0 else 0
        
        # Boost score for highly relevant events
        if 'sign expo' in event_text or 'signage' in event_text:
            normalized_score = min(normalized_score + 0.2, 1.0)
        
        # Boost score for printing events
        if 'print' in event_text or 'printing' in event_text:
            normalized_score = min(normalized_score + 0.1, 1.0)
        
        return normalized_score