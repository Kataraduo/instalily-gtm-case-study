"""Lead Scorer Module for DuPont Tedlar Sales Lead Generation System

This module is responsible for scoring and prioritizing leads based on
company and stakeholder information.
"""

import os
import logging
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.config.config import (
    OUTPUT_DATA_DIR,
    SCORING_WEIGHTS
)


class LeadScorer:
    """Class for scoring and prioritizing leads"""
    
    def __init__(self):
        """Initialize the LeadScorer with default settings"""
        # Ensure output directory exists
        self.output_dir = OUTPUT_DATA_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Scoring weights from config
        self.weights = SCORING_WEIGHTS
    
    def score_leads(self, companies_df, stakeholders_df):
        """Score and prioritize leads based on company and stakeholder information
        
        Args:
            companies_df (pandas.DataFrame): DataFrame containing enriched company information
            stakeholders_df (pandas.DataFrame): DataFrame containing stakeholder information
            
        Returns:
            tuple: (scored_companies_df, scored_stakeholders_df, leads_df)
        """
        self.logger.info(f"Scoring {len(companies_df)} companies and {len(stakeholders_df)} stakeholders")
        
        # Score companies
        scored_companies_df = self._score_companies(companies_df)
        
        # Score stakeholders
        scored_stakeholders_df = self._score_stakeholders(stakeholders_df, scored_companies_df)
        
        # Generate leads by combining company and stakeholder scores
        leads_df = self._generate_leads(scored_companies_df, scored_stakeholders_df)
        
        # Save leads data for dashboard
        leads_df.to_csv(self.output_dir / 'dashboard_data.csv', index=False)
        self.logger.info(f"Saved {len(leads_df)} leads to dashboard_data.csv")
        
        return scored_companies_df, scored_stakeholders_df, leads_df
    
    def _score_companies(self, companies_df):
        """Score companies based on relevance and fit
        
        Args:
            companies_df (pandas.DataFrame): DataFrame containing company information
            
        Returns:
            pandas.DataFrame: DataFrame containing scored companies
        """
        # Create a copy of the DataFrame to avoid modifying the original
        scored_df = companies_df.copy()
        
        # Ensure relevance_score exists
        if 'relevance_score' not in scored_df.columns:
            self.logger.warning("No relevance_score found in companies_df, calculating basic score")
            scored_df['relevance_score'] = scored_df.apply(self._calculate_basic_relevance, axis=1)
        
        # Calculate company size score
        scored_df['size_score'] = scored_df.apply(self._calculate_size_score, axis=1)
        
        # Calculate industry relevance score
        scored_df['industry_score'] = scored_df.apply(self._calculate_industry_score, axis=1)
        
        # Calculate product fit score
        scored_df['product_fit_score'] = scored_df.apply(self._calculate_product_fit, axis=1)
        
        # Calculate overall company score
        scored_df['company_score'] = (
            scored_df['size_score'] * self.weights['company_size'] +
            scored_df['industry_score'] * self.weights['industry_relevance'] +
            scored_df['product_fit_score'] * self.weights['product_fit']
        )
        
        # Normalize scores to range 0-1
        max_score = scored_df['company_score'].max()
        if max_score > 0:
            scored_df['company_score'] = scored_df['company_score'] / max_score
        
        # Round scores to 2 decimal places
        scored_df['company_score'] = scored_df['company_score'].round(2)
        
        return scored_df
    
    def _score_stakeholders(self, stakeholders_df, companies_df):
        """Score stakeholders based on decision-making power and company score
        
        Args:
            stakeholders_df (pandas.DataFrame): DataFrame containing stakeholder information
            companies_df (pandas.DataFrame): DataFrame containing scored company information
            
        Returns:
            pandas.DataFrame: DataFrame containing scored stakeholders
        """
        # Create a copy of the DataFrame to avoid modifying the original
        scored_df = stakeholders_df.copy()
        
        # Ensure decision_making_power exists
        if 'decision_making_power' not in scored_df.columns:
            self.logger.warning("No decision_making_power found in stakeholders_df, calculating from title")
            scored_df['decision_making_power'] = scored_df.apply(self._calculate_decision_power_from_title, axis=1)
        
        # Merge with company scores
        if 'company_score' in companies_df.columns:
            # Create a mapping of company names to scores
            company_scores = dict(zip(companies_df['name'], companies_df['company_score']))
            
            # Add company score to stakeholders
            scored_df['company_score'] = scored_df['company'].map(company_scores)
            
            # Fill missing values with median
            median_score = companies_df['company_score'].median()
            scored_df['company_score'] = scored_df['company_score'].fillna(median_score)
        else:
            # If no company scores available, use a default value
            scored_df['company_score'] = 0.5
        
        # Calculate stakeholder priority score
        scored_df['stakeholder_score'] = (
            scored_df['decision_making_power'] * self.weights['decision_making_power'] +
            scored_df['company_score'] * (1 - self.weights['decision_making_power'])
        )
        
        # Normalize scores to range 0-1
        max_score = scored_df['stakeholder_score'].max()
        if max_score > 0:
            scored_df['stakeholder_score'] = scored_df['stakeholder_score'] / max_score
        
        # Round scores to 2 decimal places
        scored_df['stakeholder_score'] = scored_df['stakeholder_score'].round(2)
        
        return scored_df
    
    def _generate_leads(self, companies_df, stakeholders_df):
        """Generate leads by combining company and stakeholder information
        
        Args:
            companies_df (pandas.DataFrame): DataFrame containing scored company information
            stakeholders_df (pandas.DataFrame): DataFrame containing scored stakeholder information
            
        Returns:
            pandas.DataFrame: DataFrame containing leads with combined scores
        """
        # Merge stakeholders with company information
        leads_df = pd.merge(
            stakeholders_df,
            companies_df[['name', 'industry', 'company_size', 'company_score', 'relevance_score', 'products', 'materials', 'target_markets']],
            left_on='company',
            right_on='name',
            suffixes=('', '_company')
        )
        
        # Calculate final lead score
        leads_df['lead_score'] = (leads_df['company_score'] * 0.6) + (leads_df['stakeholder_score'] * 0.4)
        
        # Round scores to 2 decimal places
        leads_df['lead_score'] = leads_df['lead_score'].round(2)
        
        # Sort by lead score in descending order
        leads_df = leads_df.sort_values('lead_score', ascending=False)
        
        # Add lead tier based on score
        leads_df['tier'] = pd.cut(
            leads_df['lead_score'],
            bins=[0, 0.3, 0.6, 1.0],
            labels=['Tier 3', 'Tier 2', 'Tier 1']
        )
        
        # Add lead ID
        leads_df['lead_id'] = [f"LEAD-{i:04d}" for i in range(1, len(leads_df) + 1)]
        
        # Select and reorder columns for dashboard
        dashboard_columns = [
            'lead_id', 'name', 'title', 'company', 'email', 'linkedin_url',
            'lead_score', 'tier', 'company_score', 'stakeholder_score',
            'decision_making_power', 'industry', 'company_size',
            'products', 'materials', 'target_markets'
        ]
        
        # Only include columns that exist in the DataFrame
        existing_columns = [col for col in dashboard_columns if col in leads_df.columns]
        
        return leads_df[existing_columns]
    
    def _calculate_basic_relevance(self, company):
        """Calculate a basic relevance score for a company
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            float: Relevance score between 0 and 1
        """
        score = 0.5  # Default score
        
        # Check if industry is relevant
        if 'industry' in company:
            industry = str(company['industry']).lower()
            if any(keyword in industry for keyword in ['sign', 'graphic', 'print', 'display', 'advertising']):
                score += 0.3
        
        # Check if description mentions relevant keywords
        if 'description' in company:
            description = str(company['description']).lower()
            relevant_keywords = ['sign', 'graphic', 'print', 'display', 'banner', 'billboard', 'exhibit', 'vinyl', 'pvc', 'film']
            matches = sum(1 for keyword in relevant_keywords if keyword in description)
            score += min(matches * 0.05, 0.2)  # Up to 0.2 points for relevant keywords
        
        return min(score, 1.0)  # Cap at 1.0
    
    def _calculate_size_score(self, company):
        """Calculate score based on company size
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            float: Size score between 0 and 1
        """
        if 'company_size' in company:
            size = str(company['company_size']).lower()
            
            if size == 'large':
                return 1.0
            elif size == 'medium':
                return 0.7
            elif size == 'small':
                return 0.4
            elif size == 'micro':
                return 0.2
        
        # If employees count is available
        if 'employees' in company and company['employees']:
            try:
                employees = int(company['employees'])
                
                if employees >= 1000:
                    return 1.0
                elif employees >= 250:
                    return 0.8
                elif employees >= 50:
                    return 0.6
                elif employees >= 10:
                    return 0.4
                else:
                    return 0.2
            except (ValueError, TypeError):
                pass
        
        # Default score if no size information is available
        return 0.5
    
    def _calculate_industry_score(self, company):
        """Calculate score based on industry relevance
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            float: Industry score between 0 and 1
        """
        if 'industry' not in company:
            return 0.5  # Default score
        
        industry = str(company['industry']).lower()
        
        # Highly relevant industries
        if any(keyword in industry for keyword in ['sign', 'signage', 'display']):
            return 1.0
        
        # Very relevant industries
        if any(keyword in industry for keyword in ['graphic', 'print', 'advertising']):
            return 0.8
        
        # Somewhat relevant industries
        if any(keyword in industry for keyword in ['marketing', 'media', 'visual', 'design', 'exhibition']):
            return 0.6
        
        # Less relevant but still potential industries
        if any(keyword in industry for keyword in ['manufacturing', 'production', 'retail', 'construction']):
            return 0.4
        
        # Not very relevant industries
        return 0.2
    
    def _calculate_product_fit(self, company):
        """Calculate score based on product fit for Tedlar
        
        Args:
            company (pandas.Series): Company information
            
        Returns:
            float: Product fit score between 0 and 1
        """
        score = 0.5  # Default score
        
        # Check products
        if 'products' in company and company['products']:
            products = company['products']
            if isinstance(products, str):
                products = [products]
            
            relevant_products = ['sign', 'banner', 'display', 'billboard', 'graphic', 'wrap', 'exhibit']
            for product in products:
                product = str(product).lower()
                if any(rp in product for rp in relevant_products):
                    score += 0.1  # 0.1 points per relevant product, up to 0.3
                    if score >= 0.8:
                        break
        
        # Check materials
        if 'materials' in company and company['materials']:
            materials = company['materials']
            if isinstance(materials, str):
                materials = [materials]
            
            relevant_materials = ['vinyl', 'pvc', 'plastic', 'film', 'composite']
            for material in materials:
                material = str(material).lower()
                if any(rm in material for rm in relevant_materials):
                    score += 0.1  # 0.1 points per relevant material, up to 0.3
                    if score >= 1.0:
                        break
        
        return min(score, 1.0)  # Cap at 1.0
    
    def _calculate_decision_power_from_title(self, stakeholder):
        """Calculate decision making power based on job title
        
        Args:
            stakeholder (pandas.Series): Stakeholder information
            
        Returns:
            float: Decision making power score between 0 and 1
        """
        if 'title' not in stakeholder or not stakeholder['title']:
            return 0.5  # Default score
        
        title = str(stakeholder['title']).lower()
        
        # Highest decision power for executive roles
        if any(keyword in title for keyword in ['ceo', 'chief', 'president', 'owner', 'founder', 'managing director']):
            return 1.0
        
        # High decision power for director and VP roles
        if any(keyword in title for keyword in ['director', 'vp', 'vice president', 'head']):
            return 0.8
        
        # Medium decision power for manager roles
        if any(keyword in title for keyword in ['manager', 'lead', 'senior', 'principal']):
            return 0.6
        
        # Lower decision power for other roles
        return 0.4