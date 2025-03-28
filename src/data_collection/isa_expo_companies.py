"""ISA Sign Expo Companies Module

This module provides a list of real companies from the ISA Sign Expo 2025 event.
These companies will be used as real data for the lead generation system.
"""

import pandas as pd
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent.parent.parent

from src.config.config import OUTPUT_DATA_DIR

class ISAExpoCompanies:
    """Class for providing real company data from ISA Sign Expo 2025"""
    
    def __init__(self):
        """Initialize the ISAExpoCompanies"""
        # Ensure output directories exist
        self.output_dir = OUTPUT_DATA_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
    def get_companies(self):
        """Get real company data from ISA Sign Expo 2025
        
        Returns:
            pandas.DataFrame: DataFrame containing company information
        """
        self.logger.info("Getting real company data from ISA Sign Expo 2025")
        
        # List of companies from ISA Sign Expo 2025
        companies = [
            {
                'name': 'CUTWORX USA',
                'booth': '2637',
                'description': 'CUTWORX USA offers a complete line of finishing solutions for all your printing, cutting, laminating, and textile needs.',
                'website': 'https://www.cutworxusa.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.9
            },
            {
                'name': 'General Formulations',
                'booth': '1937',
                'description': 'General Formulations® (GF) is a global manufacturer of pressure-sensitive print media headquartered in the USA, since 1953.',
                'website': 'https://www.generalformulations.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.85
            },
            {
                'name': 'Laguna Tools Inc.',
                'booth': '1021',
                'description': 'For over four decades, Laguna Tools has been a pioneer in the machinery industry, delivering innovative solutions.',
                'website': 'https://www.lagunatools.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.8
            },
            {
                'name': 'Lintec of America, Inc.',
                'booth': '2364',
                'description': 'Lintec Corporation is a premier supplier of pressure sensitive films and specialty media.',
                'website': 'https://www.lintecofamerica.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.85
            },
            {
                'name': 'Signage Details',
                'booth': '3813',
                'description': 'Subscribe today for unlimited access to proven, industry-standard, permit-ready section details for fabricating and installing commercial signs.',
                'website': 'https://www.signagedetails.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.75
            },
            {
                'name': '3A Composites USA, Inc.',
                'booth': '1222',
                'description': '3A Composites USA specializes in the manufacturing of leading composite substrates for the display, graphic arts, signage & framing industries.',
                'website': 'https://www.3acompositesusa.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.8
            },
            {
                'name': '3M Commercial Solutions',
                'booth': '4725',
                'description': '3M Commercial Graphics helps customers worldwide build brands by providing total large-format graphics and light management solutions.',
                'website': 'https://www.3m.com/3M/en_US/commercial-solutions-us',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.9
            },
            {
                'name': 'A.R.K. Ramos Foundry & Mfg. Co.',
                'booth': '4549',
                'description': 'A.R.K. Ramos manufactures cast and etched aluminum, brass, and bronze plaques.',
                'website': 'https://www.arkramos.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.7
            },
            {
                'name': 'Abitech',
                'booth': '4618',
                'description': 'Abitech is a distinguished wholesale distributor specializing in signage materials and graphics.',
                'website': 'https://www.abitech.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.75
            },
            {
                'name': 'ADMAX Exhibit & Display Ltd.',
                'booth': '2369, 4018',
                'description': 'ADMAX specializes in exhibit and display solutions for trade shows and events.',
                'website': 'https://www.admaxdisplays.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.7
            },
            {
                'name': 'Advanced Greig Laminators, Inc.',
                'booth': '4749',
                'description': 'Advanced Greig Laminators specializes in lamination solutions for the graphics industry.',
                'website': 'https://www.aglaminators.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.8
            },
            {
                'name': 'Advantage Innovations, Inc',
                'booth': '2445',
                'description': 'Advantage Innovations provides innovative solutions for the signage and graphics industry.',
                'website': 'https://www.advantageinnovations.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.75
            },
            {
                'name': 'Aludecor',
                'booth': '323',
                'description': 'Aludecor specializes in aluminum composite panels for signage and architectural applications.',
                'website': 'https://www.aludecor.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.8
            },
            {
                'name': 'Arlon Graphics',
                'booth': '3031, 3129',
                'description': 'Arlon Graphics is a global leader in graphic films and pressure-sensitive adhesive films.',
                'website': 'https://www.arlon.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.9
            },
            {
                'name': 'Avery Dennison Graphics Solutions',
                'booth': '3837',
                'description': 'Avery Dennison Graphics Solutions provides materials and solutions for graphics applications.',
                'website': 'https://graphics.averydennison.com',
                'industry': 'Graphics & Signage',
                'source_type': 'event',
                'source_event': 'ISA Sign Expo 2025',
                'relevance_score': 0.9
            }
        ]
        
        # Create DataFrame
        companies_df = pd.DataFrame(companies)
        
        # Save to CSV
        output_file = self.output_dir / 'isa_expo_companies.csv'
        companies_df.to_csv(output_file, index=False)
        self.logger.info(f"Saved {len(companies_df)} companies to isa_expo_companies.csv")
        
        return companies_df


def main():
    """Main function to demonstrate the ISA Expo Companies module"""
    isa_companies = ISAExpoCompanies()
    companies_df = isa_companies.get_companies()
    print(f"Successfully loaded {len(companies_df)} companies from ISA Sign Expo 2025")
    print("\nSample of companies:")
    print(companies_df[['name', 'booth', 'description']].head())


if __name__ == "__main__":
    main()