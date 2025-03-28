"""ISA Sign Expo Graphics Parser Module

This module is responsible for parsing company information from the ISA Sign Expo 2025 event,
specifically targeting companies related to the 'graphics' keyword. It can either scrape
the data directly from the website or parse it from provided text data.
"""

import re
import pandas as pd
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent.parent.parent

from src.config.config import OUTPUT_DATA_DIR

class ISAGraphicsParser:
    """Class for parsing company information from ISA Sign Expo with 'graphics' keyword"""
    
    def __init__(self):
        """Initialize the ISAGraphicsParser"""
        # Ensure output directories exist
        self.output_dir = OUTPUT_DATA_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
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
            exhibitor_blocks = re.split(r'\n(?=[A-Z][\w\s.,&]+\n)', section)
            
            for block in exhibitor_blocks:
                # Skip headers and empty blocks
                if not block.strip() or any(header in block for header in ["ExhibitorSummaryBoothAdd to Planner", "Featured Exhibitors", "See Results on Floor Plan", "Results for Keyword", "Grid List"]):
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
                    if re.match(r'^[\w\d, ]+$', booth):
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
                
                all_exhibitors.append(exhibitor)
        
        # Create DataFrame from all exhibitors
        exhibitors_df = pd.DataFrame(all_exhibitors)
        
        # Save raw exhibitors data
        if not exhibitors_df.empty:
            output_file = self.output_dir / 'isa_expo_graphics_companies.csv'
            exhibitors_df.to_csv(output_file, index=False)
            self.logger.info(f"Saved {len(exhibitors_df)} exhibitors from text data to isa_expo_graphics_companies.csv")
        
        return exhibitors_df


def main():
    """Main function to demonstrate the ISA Graphics Parser"""
    parser = ISAGraphicsParser()
    
    # Sample text data from ISA Sign Expo 2025 with graphics keyword search
    sample_text = """100 Results for Keyword: "graphics"
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
4618"""
    
    # Parse the sample text
    exhibitors_df = parser.parse_exhibitor_text(sample_text)
    
    # Display results
    print(f"Successfully parsed {len(exhibitors_df)} companies from ISA Sign Expo text data")
    print("\nSample of parsed companies:")
    print(exhibitors_df[['name', 'booth', 'description']].head())


if __name__ == "__main__":
    main()