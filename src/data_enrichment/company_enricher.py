"""
Company data enrichment module
"""
import pandas as pd
import logging
import time
import random
from typing import Dict, Any, List
import requests
from bs4 import BeautifulSoup
from ..config.config import ICP_CRITERIA, TARGET_PRODUCT

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompanyEnricher:
    """Company data enrichment class"""
    
    def __init__(self):
        """Initialize company enricher"""
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.industry_keywords = ICP_CRITERIA['industry_keywords']
    
    def enrich_companies_data(self, companies_df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich company data with additional information
        
        Args:
            companies_df: Companies DataFrame
            
        Returns:
            Enriched companies DataFrame
        """
        logger.info("Enriching company data")
        
        if companies_df.empty:
            logger.warning("No companies to enrich")
            return companies_df
        
        enriched_companies = []
        
        for _, company in companies_df.iterrows():
            try:
                enriched_company = self._enrich_company(company.to_dict())
                enriched_companies.append(enriched_company)
                # Add random delay to avoid rate limiting
                time.sleep(random.uniform(0.5, 1.5))
            except Exception as e:
                logger.error(f"Error enriching company {company['name']}: {str(e)}")
                enriched_companies.append(company.to_dict())
        
        enriched_df = pd.DataFrame(enriched_companies)
        
        # Calculate ICP score for each company
        enriched_df['company_icp_score'] = enriched_df.apply(self._calculate_icp_score, axis=1)
        
        # Calculate relevance to Tedlar for each company
        enriched_df['relevance_to_tedlar'] = enriched_df.apply(self._calculate_tedlar_relevance, axis=1)
        
        # Sort by ICP score
        enriched_df = enriched_df.sort_values(by='company_icp_score', ascending=False)
        
        logger.info(f"Enriched data for {len(enriched_df)} companies")
        return enriched_df
    
    def _enrich_company(self, company: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a single company with additional information
        
        Args:
            company: Company information dictionary
            
        Returns:
            Enriched company dictionary
        """
        logger.info(f"Enriching company: {company['name']}")
        
        # In a real implementation, this would use APIs like Clearbit, Apollo, etc.
        # For this case study, we'll add mock enrichment data
        
        # Add location if not present
        if 'location' not in company:
            company['location'] = random.choice([
                'New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX', 'Phoenix, AZ',
                'Philadelphia, PA', 'San Antonio, TX', 'San Diego, CA', 'Dallas, TX', 'San Jose, CA'
            ])
        
        # Add year founded if not present
        if 'year_founded' not in company:
            company['year_founded'] = random.randint(1980, 2015)
        
        # Add description if not present
        if 'description' not in company:
            company['description'] = f"{company['name']} is a leading provider of {', '.join(company.get('products', ['signage and graphics solutions']))} for the {', '.join(company.get('target_markets', ['retail and corporate']))} markets."
        
        # Add technologies used
        if 'technologies' not in company:
            tech_options = [
                'Digital Printing', 'UV Printing', 'Screen Printing', 'Vinyl Cutting',
                'CNC Routing', 'Laser Cutting', 'Lamination', 'Thermoforming'
            ]
            num_techs = random.randint(2, 5)
            company['technologies'] = random.sample(tech_options, num_techs)
        
        # Add materials used
        if 'materials' not in company:
            material_options = [
                'Vinyl', 'Acrylic', 'Aluminum', 'PVC', 'Polycarbonate',
                'Fabric', 'Foam Board', 'Corrugated Plastic', 'Glass'
            ]
            num_materials = random.randint(3, 6)
            company['materials'] = random.sample(material_options, num_materials)
        
        # Add social media profiles
        if 'social_media' not in company:
            company_name_slug = company['name'].lower().replace(' ', '')
            company['social_media'] = {
                'linkedin': f"https://www.linkedin.com/company/{company_name_slug}",
                'twitter': f"https://twitter.com/{company_name_slug}",
                'facebook': f"https://www.facebook.com/{company_name_slug}"
            }
        
        # Add competitors
        if 'competitors' not in company:
            competitor_options = [
                'SignageCorp', 'GraphicsPlus', 'DisplayMasters', 'VisualSolutions',
                'PrintPros', 'SignageExperts', 'GraphicGurus', 'DisplayInnovators'
            ]
            num_competitors = random.randint(2, 4)
            company['competitors'] = random.sample(competitor_options, num_competitors)
        
        return company
    
    def _calculate_icp_score(self, company: pd.Series) -> float:
        """
        Calculate ICP (Ideal Customer Profile) score for a company
        
        Args:
            company: Company Series from DataFrame
            
        Returns:
            ICP score (0-100)
        """
        score = 0
        max_score = 100
        
        # Company size score (0-25)
        company_size = company.get('company_size', 0)
        min_size = ICP_CRITERIA['company_size_min']
        if company_size >= min_size:
            size_score = min(25, (company_size / min_size) * 15)
        else:
            size_score = (company_size / min_size) * 15
        score += size_score
        
        # Annual revenue score (0-25)
        annual_revenue = company.get('annual_revenue', 0)
        min_revenue = ICP_CRITERIA['annual_revenue_min']
        if annual_revenue >= min_revenue:
            revenue_score = min(25, (annual_revenue / min_revenue) * 15)
        else:
            revenue_score = (annual_revenue / min_revenue) * 15
        score += revenue_score
        
        # Industry keywords score (0-25)
        keyword_score = 0
        company_description = company.get('description', '').lower()
        for keyword in self.industry_keywords:
            if keyword.lower() in company_description:
                keyword_score += 5
        
        products = company.get('products', [])
        for product in products:
            for keyword in self.industry_keywords:
                if keyword.lower() in product.lower():
                    keyword_score += 3
        
        score += min(25, keyword_score)
        
        # Materials used score (0-25)
        materials_score = 0
        materials = company.get('materials', [])
        relevant_materials = ['Vinyl', 'PVC', 'Polycarbonate', 'Plastic', 'Laminate']
        
        for material in materials:
            if any(rm.lower() in material.lower() for rm in relevant_materials):
                materials_score += 5
        
        score += min(25, materials_score)
        
        # Ensure score is between 0 and 100
        return max(0, min(100, score))
    
    def _calculate_tedlar_relevance(self, company: pd.Series) -> str:
        """
        Calculate relevance of Tedlar to the company
        
        Args:
            company: Company Series from DataFrame
            
        Returns:
            Relevance description string
        """
        # Check if company uses relevant materials
        materials = company.get('materials', [])
        uses_relevant_materials = any(material.lower() in ['vinyl', 'pvc', 'polycarbonate', 'plastic', 'laminate'] 
                                     for material in materials)
        
        # Check if company is in relevant markets
        target_markets = company.get('target_markets', [])
        relevant_markets = ['outdoor', 'architectural', 'transportation', 'retail']
        in_relevant_markets = any(market.lower() in relevant_markets for market in target_markets)
        
        # Check if company uses relevant technologies
        technologies = company.get('technologies', [])
        relevant_techs = ['printing', 'lamination', 'thermoforming']
        uses_relevant_techs = any(any(tech.lower() in technology.lower() for tech in relevant_techs) 
                                 for technology in technologies)
        
        # Generate relevance description based on findings
        relevance_points = []
        
        if uses_relevant_materials:
            relevance_points.append(f"Your use of {', '.join(materials)} could be enhanced with Tedlar's protective properties")
        
        if in_relevant_markets:
            relevance_points.append(f"Tedlar can help your products stand out in the {', '.join(target_markets)} markets")
        
        if uses_relevant_techs:
            relevance_points.append(f"Tedlar is compatible with your {', '.join(technologies)} processes")
        
        if company.get('company_icp_score', 0) > 70:
            relevance_points.append("Your company profile strongly aligns with our ideal customer profile")
        
        # If no specific relevance found, provide generic statement
        if not relevance_points:
            return "Your company may benefit from Tedlar's durability and weather resistance properties for your signage and graphics applications"
        
        # Return combined relevance points
        return ". ".join(relevance_points) + "."