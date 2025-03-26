"""
Stakeholder finder module - For identifying decision makers at target companies
"""
import pandas as pd
import logging
import time
import random
from typing import List, Dict, Any
import requests
from bs4 import BeautifulSoup
from ..config.config import ICP_CRITERIA

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StakeholderFinder:
    """Stakeholder finder class"""
    
    def __init__(self):
        """Initialize stakeholder finder"""
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.decision_maker_titles = ICP_CRITERIA['decision_maker_titles']
    
    def find_stakeholders_for_companies(self, companies_df: pd.DataFrame) -> pd.DataFrame:
        """
        Find stakeholders for companies
        
        Args:
            companies_df: Companies DataFrame
            
        Returns:
            Stakeholders DataFrame
        """
        logger.info("Finding stakeholders for companies")
        
        if companies_df.empty:
            logger.warning("No companies to find stakeholders for")
            return pd.DataFrame()
        
        stakeholders = []
        
        for _, company in companies_df.iterrows():
            try:
                company_stakeholders = self._find_stakeholders_for_company(company)
                stakeholders.extend(company_stakeholders)
                # Add random delay to avoid rate limiting
                time.sleep(random.uniform(0.5, 1.5))
            except Exception as e:
                logger.error(f"Error finding stakeholders for company {company['name']}: {str(e)}")
        
        stakeholders_df = pd.DataFrame(stakeholders)
        
        if stakeholders_df.empty:
            logger.warning("No stakeholders found")
            return pd.DataFrame()
        
        # Add company ICP score to stakeholders
        stakeholders_df = self._add_company_data_to_stakeholders(stakeholders_df, companies_df)
        
        logger.info(f"Found {len(stakeholders_df)} stakeholders")
        return stakeholders_df
    
    def _find_stakeholders_for_company(self, company: pd.Series) -> List[Dict[str, Any]]:
        """
        Find stakeholders for a single company
        
        Args:
            company: Company Series from DataFrame
            
        Returns:
            List of stakeholder dictionaries
        """
        logger.info(f"Finding stakeholders for company: {company['name']}")
        
        # In a real implementation, this would use APIs like LinkedIn, Apollo, etc.
        # For this case study, we'll generate mock stakeholder data
        
        company_name = company['name']
        stakeholders = []
        
        # Generate 2-5 stakeholders per company
        num_stakeholders = random.randint(2, 5)
        
        first_names = ['John', 'Sarah', 'Michael', 'Emily', 'David', 'Jennifer', 'Robert', 'Lisa', 'William', 'Jessica']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Garcia', 'Rodriguez', 'Wilson']
        
        for i in range(num_stakeholders):
            # Generate random stakeholder data
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            name = f"{first_name} {last_name}"
            
            # Assign a relevant title
            title = random.choice(self.decision_maker_titles)
            
            # Generate email
            email_domain = company['website'].replace('https://', '').replace('http://', '').replace('www.', '')
            email = f"{first_name.lower()}.{last_name.lower()}@{email_domain}"
            
            # Generate LinkedIn URL
            linkedin_url = f"https://www.linkedin.com/in/{first_name.lower()}-{last_name.lower()}-{random.randint(10000, 99999)}"
            
            # Assign decision making power
            decision_making_power = random.choice(['High', 'Medium', 'Low'])
            
            # Create stakeholder dictionary
            stakeholder = {
                'name': name,
                'title': title,
                'company': company_name,
                'email': email,
                'phone': f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                'linkedin_url': linkedin_url,
                'decision_making_power': decision_making_power,
                'years_at_company': random.randint(1, 15),
                'previous_company': random.choice(['3M', 'Avery Dennison', 'Oracle', 'IBM', 'Microsoft', 'Adobe', 'HP', 'Dell']),
                'education': random.choice(['MBA', 'BS in Engineering', 'BS in Business', 'MS in Marketing', 'PhD']),
                'location': company.get('location', 'United States')
            }
            
            stakeholders.append(stakeholder)
        
        return stakeholders
    
    def _add_company_data_to_stakeholders(self, stakeholders_df: pd.DataFrame, companies_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add company data to stakeholders
        
        Args:
            stakeholders_df: Stakeholders DataFrame
            companies_df: Companies DataFrame
            
        Returns:
            Stakeholders DataFrame with company data
        """
        logger.info("Adding company data to stakeholders")
        
        # Create a copy to avoid modifying the original
        result_df = stakeholders_df.copy()
        
        # Ensure all necessary columns exist in the result DataFrame
        company_columns = ['industry', 'company_size', 'annual_revenue', 'year_founded', 
                          'products', 'materials', 'technologies', 'website']
        
        for col in company_columns:
            if col not in result_df.columns:
                result_df[col] = None
        
        # Create company information dictionary for quick lookup
        companies_dict = {}
        for _, company in companies_df.iterrows():
            companies_dict[company['name']] = company.to_dict()
        
        # Add company data to each stakeholder
        for idx, stakeholder in result_df.iterrows():
            company_name = stakeholder['company']
            
            if company_name in companies_dict:
                company_info = companies_dict[company_name]
                
                # Add company data to stakeholder
                for key in company_columns:
                    if key in company_info:
                        # Safely handle the assignment
                        try:
                            result_df.at[idx, key] = company_info[key]
                        except Exception as e:
                            logger.warning(f"Could not set {key} for stakeholder {stakeholder['name']}: {str(e)}")
                            # Set to None instead of failing
                            result_df.at[idx, key] = None
        
        logger.info(f"Added company data to {len(result_df)} stakeholders")
        return result_df
    
    def prioritize_stakeholders(self, stakeholders_df: pd.DataFrame) -> pd.DataFrame:
        """
        Prioritize stakeholders based on various factors
        
        Args:
            stakeholders_df: Stakeholders DataFrame
            
        Returns:
            Prioritized stakeholders DataFrame
        """
        logger.info("Prioritizing stakeholders")
        
        if stakeholders_df.empty:
            logger.warning("No stakeholders to prioritize")
            return stakeholders_df
        
        # Calculate priority score for each stakeholder
        stakeholders_df['priority_score'] = stakeholders_df.apply(self._calculate_priority_score, axis=1)
        
        # Calculate relevance score for each stakeholder
        stakeholders_df['relevance_score'] = stakeholders_df.apply(self._calculate_relevance_score, axis=1)
        
        # Sort by priority score
        prioritized_df = stakeholders_df.sort_values(by='priority_score', ascending=False)
        
        logger.info("Stakeholders prioritized")
        return prioritized_df
    
    def _calculate_priority_score(self, stakeholder: pd.Series) -> float:
        """
        Calculate priority score for a stakeholder
        
        Args:
            stakeholder: Stakeholder Series from DataFrame
            
        Returns:
            Priority score (0-100)
        """
        score = 0
        max_score = 100
        
        # Company ICP score (0-40)
        company_icp_score = stakeholder.get('company_icp_score', 0)
        score += company_icp_score * 0.4
        
        # Decision making power (0-30)
        decision_power = stakeholder.get('decision_making_power', 'Low')
        if decision_power == 'High':
            score += 30
        elif decision_power == 'Medium':
            score += 15
        else:  # Low
            score += 5
        
        # Title relevance (0-20)
        title = stakeholder.get('title', '').lower()
        title_score = 0
        for relevant_title in self.decision_maker_titles:
            if relevant_title.lower() in title:
                title_score = 20
                break
        score += title_score
        
        # Years at company (0-10)
        years_at_company = stakeholder.get('years_at_company', 0)
        years_score = min(10, years_at_company)
        score += years_score
        
        # Ensure score is between 0 and 100
        return max(0, min(100, score))
    
    def _calculate_relevance_score(self, stakeholder: pd.Series) -> str:
        """
        Calculate relevance score description for a stakeholder
        
        Args:
            stakeholder: Stakeholder Series from DataFrame
            
        Returns:
            Relevance score description
        """
        # Get stakeholder title
        title = stakeholder.get('title', '').lower()
        
        # Get company data
        company_icp_score = stakeholder.get('company_icp_score', 0)
        products = stakeholder.get('products', [])
        materials = stakeholder.get('materials', [])
        
        # Generate relevance description
        relevance_points = []
        
        # Title-based relevance
        if 'product' in title or 'innovation' in title or 'r&d' in title:
            relevance_points.append("Your role in product development aligns with our material innovation focus")
        elif 'procurement' in title or 'purchasing' in title:
            relevance_points.append("As a procurement decision-maker, you can evaluate Tedlar's cost-benefit advantages")
        elif 'technology' in title or 'technical' in title:
            relevance_points.append("Your technical expertise can help assess Tedlar's performance benefits")
        
        # Company-based relevance
        if company_icp_score > 80:
            relevance_points.append("Your company is an ideal fit for our protective film solutions")
        
        # Product-based relevance
        if any('signage' in product.lower() for product in products):
            relevance_points.append("Tedlar can enhance the durability of your signage products")
        elif any('display' in product.lower() for product in products):
            relevance_points.append("Tedlar provides UV and weather protection for your display solutions")
        
        # If no specific relevance found, provide generic statement
        if not relevance_points:
            return "Your professional background suggests you would understand the value of high-performance protective films"
        
        # Return combined relevance points
        return ". ".join(relevance_points) + "."