"""Company Enricher Module for DuPont Tedlar Sales Lead Generation System

This module is responsible for enriching company data with additional information
from various sources to improve lead scoring and personalization.
"""

import os
import logging
import pandas as pd
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
    CLEARBIT_API_KEY
)


class CompanyEnricher:
    """Class for enriching company data with additional information"""
    
    def __init__(self):
        """Initialize the CompanyEnricher with default headers and settings"""
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
        
        # Define industry keywords relevant to DuPont Tedlar
        self.industry_keywords = {
            'signage': ['sign', 'signage', 'display', 'billboard', 'banner', 'exhibit'],
            'graphics': ['graphic', 'print', 'printing', 'visual', 'design', 'creative'],
            'manufacturing': ['manufacturing', 'production', 'fabrication', 'industrial'],
            'materials': ['material', 'vinyl', 'pvc', 'film', 'plastic', 'composite'],
            'outdoor': ['outdoor', 'exterior', 'weather', 'durable', 'resistant'],
            'advertising': ['advertising', 'marketing', 'promotion', 'media', 'brand']
        }
    
    def enrich_companies(self, companies_df):
        """Enrich company data with additional information
        
        Args:
            companies_df (pandas.DataFrame): DataFrame containing company information
            
        Returns:
            pandas.DataFrame: DataFrame containing enriched company information
        """
        self.logger.info(f"Enriching data for {len(companies_df)} companies")
        
        # Create a copy of the DataFrame to avoid modifying the original
        enriched_df = companies_df.copy()
        
        # Add unique ID for each company if not already present
        if 'id' not in enriched_df.columns:
            enriched_df['id'] = [f"COMP-{i:04d}" for i in range(1, len(enriched_df) + 1)]
        
        # Enrich with Clearbit data if API key is available
        if CLEARBIT_API_KEY:
            enriched_df = self._enrich_with_clearbit(enriched_df)
        
        # Extract industry from company description and website
        enriched_df['industry'] = enriched_df.apply(self._extract_industry, axis=1)
        
        # Extract company size from available data
        enriched_df['company_size'] = enriched_df.apply(self._extract_company_size, axis=1)
        
        # Extract products and materials from company description
        enriched_df['products'] = enriched_df.apply(self._extract_products, axis=1)
        enriched_df['materials'] = enriched_df.apply(self._extract_materials, axis=1)
        
        # Extract target markets from company description
        enriched_df['target_markets'] = enriched_df.apply(self._extract_target_markets, axis=1)
        
        # Calculate relevance score for each company
        enriched_df['relevance_score'] = enriched_df.apply(self._calculate_relevance_score, axis=1)
        
        # Save enriched companies data
        enriched_df.to_csv(self.output_dir / 'companies_enriched.csv', index=False)
        self.logger.info(f"Saved {len(enriched_df)} enriched companies to companies_enriched.csv")
        
        return enriched_df
    
    def _enrich_with_clearbit(self, companies_df):
        """Enrich company data with Clearbit API
        
        Args:
            companies_df (pandas.DataFrame): DataFrame containing company information
            
        Returns:
            pandas.DataFrame: DataFrame containing enriched company information
        """
        if not CLEARBIT_API_KEY:
            return companies_df
        
        self.logger.info("Enriching companies with Clearbit API")
        
        for idx, company in companies_df.iterrows():
            company_domain = self._extract_domain(company.get('website', ''))
            
            if not company_domain:
                continue
            
            try:
                url = f"https://company.clearbit.com/v2/companies/find?domain={company_domain}"
                headers = {
                    'Authorization': f'Bearer {CLEARBIT_API_KEY}',
                    **self.headers
                }
                
                response = requests.get(url, headers=headers, timeout=self.timeout)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Update company information with Clearbit data
                    if 'name' in data:
                        companies_df.at[idx, 'name'] = data['name']
                    
                    if 'description' in data:
                        companies_df.at[idx, 'description'] = data['description']
                    
                    if 'category' in data and 'industry' in data['category']:
                        companies_df.at[idx, 'industry'] = data['category']['industry']
                    
                    if 'metrics' in data:
                        if 'employees' in data['metrics']:
                            companies_df.at[idx, 'employees'] = data['metrics']['employees']
                        
                        if 'annualRevenue' in data['metrics']:
                            companies_df.at[idx, 'annual_revenue'] = data['metrics']['annualRevenue']
                    
                    if 'tags' in data:
                        companies_df.at[idx, 'tags'] = data['tags']
                    
                    if 'location' in data:
                        location_parts = []
                        if 'city' in data['location']:
                            location_parts.append(data['location']['city'])
                        if 'state' in data['location']:
                            location_parts.append(data['location']['state'])
                        if 'country' in data['location']:
                            location_parts.append(data['location']['country'])
                        
                        if location_parts:
                            companies_df.at[idx, 'location'] = ', '.join(location_parts)
                
                # Respect rate limits but use a minimal delay to speed up processing
                time.sleep(self.delay / 2)  # Use half the configured delay to speed up processing
                
            except Exception as e:
                self.logger.error(f"Error enriching {company.get('name', '')} with Clearbit: {str(e)}")
        
        return companies_df
    
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
    
    def _extract_industry(self, company):
        """Extract industry from company description and website
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            str: Extracted industry
        """
        # If industry is already available, return it
        if 'industry' in company and company['industry']:
            return company['industry']
        
        # Extract industry from description
        description = str(company.get('description', '')).lower()
        website = str(company.get('website', '')).lower()
        
        # Combine description and website for analysis
        text = f"{description} {website}"
        
        # Check for industry keywords
        industry_scores = {}
        
        for industry, keywords in self.industry_keywords.items():
            score = 0
            for keyword in keywords:
                if keyword in text:
                    score += 1
            
            if score > 0:
                industry_scores[industry] = score
        
        # Return the industry with the highest score
        if industry_scores:
            return max(industry_scores, key=industry_scores.get)
        
        # Default industry if none detected
        return 'Graphics and Signage'
    
    def _extract_company_size(self, company):
        """Extract company size from available data
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            str: Company size category
        """
        # If employees count is available, use it to determine company size
        if 'employees' in company and company['employees']:
            try:
                employees = int(company['employees'])
                
                if employees < 10:
                    return 'Micro'
                elif employees < 50:
                    return 'Small'
                elif employees < 250:
                    return 'Medium'
                else:
                    return 'Large'
            except (ValueError, TypeError):
                pass
        
        # If annual revenue is available, use it to determine company size
        if 'annual_revenue' in company and company['annual_revenue']:
            try:
                revenue = float(company['annual_revenue'])
                
                if revenue < 1000000:  # Less than $1M
                    return 'Micro'
                elif revenue < 10000000:  # $1M - $10M
                    return 'Small'
                elif revenue < 50000000:  # $10M - $50M
                    return 'Medium'
                else:  # More than $50M
                    return 'Large'
            except (ValueError, TypeError):
                pass
        
        # If no reliable data is available, make an educated guess based on description
        description = str(company.get('description', '')).lower()
        
        if any(keyword in description for keyword in ['fortune 500', 'global', 'multinational', 'enterprise', 'corporation']):
            return 'Large'
        elif any(keyword in description for keyword in ['medium', 'growing', 'established']):
            return 'Medium'
        elif any(keyword in description for keyword in ['small', 'startup', 'boutique', 'family']):
            return 'Small'
        
        # Default to medium if no information is available
        return 'Medium'
    
    def _extract_products(self, company):
        """Extract products from company description
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            list: List of extracted products
        """
        description = str(company.get('description', '')).lower()
        
        # Define product keywords relevant to graphics and signage industry
        product_keywords = [
            'signs', 'banners', 'displays', 'billboards', 'posters', 'graphics',
            'wraps', 'vehicle wraps', 'wall graphics', 'window graphics', 'floor graphics',
            'trade show displays', 'exhibits', 'digital signage', 'led signs',
            'channel letters', 'monument signs', 'wayfinding', 'architectural signage'
        ]
        
        # Extract products mentioned in the description
        products = []
        for product in product_keywords:
            if product in description:
                products.append(product.title())  # Capitalize first letter of each word
        
        # If no products found, add default products based on industry
        if not products:
            industry = company.get('industry', '')
            # Ensure industry is a string before calling lower()
            if not isinstance(industry, str):
                industry = str(industry) if industry is not None else ''
            industry = industry.lower()
            
            if 'sign' in industry or 'display' in industry:
                products = ['Signs', 'Displays']
            elif 'print' in industry or 'graphic' in industry:
                products = ['Graphics', 'Printed Materials']
            elif 'advertising' in industry or 'marketing' in industry:
                products = ['Advertising Displays', 'Marketing Materials']
            else:
                products = ['Graphics Products', 'Signage Solutions']
        
        return products
    
    def _extract_materials(self, company):
        """Extract materials from company description
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            list: List of extracted materials
        """
        description = str(company.get('description', '')).lower()
        
        # Define material keywords relevant to graphics and signage industry
        material_keywords = [
            'vinyl', 'pvc', 'acrylic', 'aluminum', 'metal', 'plastic', 'composite',
            'fabric', 'canvas', 'film', 'adhesive', 'foam', 'wood', 'glass', 'led',
            'polycarbonate', 'coroplast', 'dibond', 'alucobond', 'sintra'
        ]
        
        # Extract materials mentioned in the description
        materials = []
        for material in material_keywords:
            if material in description:
                materials.append(material.title())  # Capitalize first letter
        
        # If no materials found, add default materials based on industry
        if not materials:
            industry = company.get('industry', '').lower()
            products = company.get('products', [])
            
            if isinstance(products, str):
                products = [products]
            
            if 'sign' in industry or any('sign' in str(p).lower() for p in products):
                materials = ['Vinyl', 'Aluminum', 'Acrylic']
            elif 'print' in industry or any('print' in str(p).lower() for p in products):
                materials = ['Vinyl', 'Film', 'Paper']
            elif 'display' in industry or any('display' in str(p).lower() for p in products):
                materials = ['Fabric', 'Vinyl', 'Aluminum']
            else:
                materials = ['Vinyl', 'Plastic', 'Composite']
        
        return materials
    
    def _extract_target_markets(self, company):
        """Extract target markets from company description
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            list: List of extracted target markets
        """
        description = str(company.get('description', '')).lower()
        
        # Define target market keywords relevant to graphics and signage industry
        market_keywords = {
            'Retail': ['retail', 'store', 'shop', 'mall', 'boutique'],
            'Corporate': ['corporate', 'office', 'business', 'enterprise', 'company'],
            'Education': ['education', 'school', 'university', 'college', 'campus'],
            'Healthcare': ['healthcare', 'hospital', 'medical', 'clinic', 'doctor'],
            'Hospitality': ['hospitality', 'hotel', 'restaurant', 'resort', 'cafe'],
            'Transportation': ['transportation', 'airport', 'transit', 'bus', 'train'],
            'Government': ['government', 'municipal', 'city', 'state', 'federal'],
            'Events': ['event', 'exhibition', 'trade show', 'conference', 'convention'],
            'Outdoor Advertising': ['outdoor', 'billboard', 'street', 'roadside', 'highway']
        }
        
        # Extract target markets mentioned in the description
        markets = []
        for market, keywords in market_keywords.items():
            for keyword in keywords:
                if keyword in description:
                    markets.append(market)
                    break  # Only add each market once
        
        # If no target markets found, add default markets based on industry
        if not markets:
            industry = company.get('industry', '').lower()
            
            if 'retail' in industry or 'store' in industry:
                markets = ['Retail', 'Corporate']
            elif 'advertising' in industry or 'marketing' in industry:
                markets = ['Corporate', 'Retail', 'Outdoor Advertising']
            elif 'exhibit' in industry or 'display' in industry:
                markets = ['Events', 'Corporate', 'Retail']
            else:
                markets = ['Corporate', 'Retail', 'Events']
        
        return markets
    
    def _calculate_relevance_score(self, company):
        """Calculate relevance score for a company based on its fit for DuPont Tedlar products
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            float: Relevance score between 0 and 1
        """
        score = 0.0
        max_score = 5.0  # Maximum possible score
        
        # Score based on industry (0-1 points)
        industry = str(company.get('industry', '')).lower()
        if 'sign' in industry or 'display' in industry:
            score += 1.0
        elif 'print' in industry or 'graphic' in industry:
            score += 0.8
        elif 'advertising' in industry or 'marketing' in industry:
            score += 0.6
        elif 'manufacturing' in industry or 'production' in industry:
            score += 0.5
        
        # Score based on products (0-1 points)
        products = company.get('products', [])
        if isinstance(products, str):
            products = [products]
        
        product_score = 0.0
        relevant_products = ['signs', 'banners', 'displays', 'billboards', 'wraps', 'graphics']
        for product in products:
            product = str(product).lower()
            if any(rp in product for rp in relevant_products):
                product_score += 0.2  # 0.2 points per relevant product, up to 1.0
        score += min(product_score, 1.0)
        
        # Score based on materials (0-1 points)
        materials = company.get('materials', [])
        if isinstance(materials, str):
            materials = [materials]
        
        material_score = 0.0
        relevant_materials = ['vinyl', 'pvc', 'plastic', 'film', 'composite']
        for material in materials:
            material = str(material).lower()
            if any(rm in material for rm in relevant_materials):
                material_score += 0.2  # 0.2 points per relevant material, up to 1.0
        score += min(material_score, 1.0)
        
        # Score based on target markets (0-1 points)
        markets = company.get('target_markets', [])
        if isinstance(markets, str):
            markets = [markets]
        
        market_score = 0.0
        relevant_markets = ['outdoor advertising', 'retail', 'events', 'transportation']
        for market in markets:
            market = str(market).lower()
            if any(rm in market for rm in relevant_markets):
                market_score += 0.25  # 0.25 points per relevant market, up to 1.0
        score += min(market_score, 1.0)
        
        # Score based on company size (0-1 points)
        company_size = str(company.get('company_size', '')).lower()
        if company_size == 'large':
            score += 1.0
        elif company_size == 'medium':
            score += 0.8
        elif company_size == 'small':
            score += 0.5
        else:  # Micro or unknown
            score += 0.3
        
        # Normalize score to range 0-1
        normalized_score = score / max_score
        
        return round(normalized_score, 2)  # Round to 2 decimal places