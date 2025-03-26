"""
Outreach message generation module
"""
import pandas as pd
import logging
import time
import random
from typing import Dict, Any, List
import openai
from ..config.config import OPENAI_API_KEY, LLM_MODEL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MessageGenerator:
    """Outreach message generation class"""
    
    def __init__(self):
        # Set OpenAI API key
        self.openai_api_key = OPENAI_API_KEY
        self.model = LLM_MODEL
    
    def generate_personalized_message(self, lead: Dict[str, Any]) -> str:
        """
        Generate personalized outreach message for a lead
        
        Args:
            lead: Lead data dictionary
            
        Returns:
            Personalized outreach message
        """
        logger.info(f"Generating personalized outreach message for {lead['name']} ({lead['company']})")
        
        # If OpenAI API key is not configured, use template message
        if not self.openai_api_key:
            return self._generate_template_message(lead)
        
        try:
            # Prepare prompt
            prompt = self._prepare_prompt(lead)
            
            # Use new OpenAI API
            from openai import OpenAI
            
            client = OpenAI(api_key=self.openai_api_key)
            
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a helpful sales assistant that creates personalized outreach messages."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=500,
                temperature=0.7
            )
            
            # Extract message from response
            message = response.choices[0].message.content.strip()
            
            return message
        except Exception as e:
            logger.error(f"Error generating message with OpenAI: {str(e)}")
            # If OpenAI generation fails, fall back to template message
            return self._generate_template_message(lead)
    
    def generate_messages_for_stakeholders(self, stakeholders_df: pd.DataFrame, companies_df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate personalized outreach messages for all stakeholders
        
        Args:
            stakeholders_df: Stakeholders DataFrame
            companies_df: Companies DataFrame
            
        Returns:
            Stakeholders DataFrame with outreach messages
        """
        result_df = stakeholders_df.copy()
        # Initialize outreach_message column as empty string
        result_df['outreach_message'] = ''
        
        # Create company information dictionary for quick lookup
        companies_dict = {}
        for _, company in companies_df.iterrows():
            companies_dict[company['name']] = company.to_dict()
        
        for idx, stakeholder in result_df.iterrows():
            try:
                # Enrich stakeholder data with company info
                stakeholder_data = stakeholder.to_dict()
                company_name = stakeholder['company']
                if company_name in companies_dict:
                    company_info = companies_dict[company_name]
                    # Add relevant company info to stakeholder data
                    stakeholder_data['company_description'] = company_info.get('description', '')
                    stakeholder_data['company_products'] = company_info.get('products', [])
                    stakeholder_data['company_materials'] = company_info.get('materials', [])
                
                message = self.generate_personalized_message(stakeholder_data)
                # Use at method to update single value
                result_df.at[idx, 'outreach_message'] = message
                
                # Add random delay to avoid API rate limits
                time.sleep(random.uniform(0.5, 2))
            except Exception as e:
                logger.error(f"Error generating message for {stakeholder['name']}: {str(e)}")
                result_df.at[idx, 'outreach_message'] = "Unable to generate message."
        
        return result_df
    
    def _prepare_prompt(self, lead: Dict[str, Any]) -> str:
        """
        Prepare prompt for OpenAI API
        
        Args:
            lead: Lead data dictionary
            
        Returns:
            Prompt string
        """
        name = lead.get('name', '')
        title = lead.get('title', '')
        company = lead.get('company', '')
        relevance_score = lead.get('relevance_score', '')
        company_description = lead.get('company_description', '')
        
        prompt = f"""
        Create a personalized sales outreach email to {name}, who is the {title} at {company}.
        
        Here's some information about the lead and their company:
        - Company: {company}
        - Relevance to our product: {relevance_score}
        - Company description: {company_description}
        
        The email should:
        1. Be concise (150-200 words)
        2. Mention DuPont Tedlar protective films as our product
        3. Reference how Tedlar can specifically help their business based on the relevance information
        4. Include a clear call to action for a brief meeting
        5. Be professional but conversational in tone
        6. Not use generic sales language
        
        Format the email with a greeting, 2-3 paragraphs of content, and a signature.
        """
        
        return prompt
    
    def _generate_template_message(self, lead: Dict[str, Any]) -> str:
        """
        Generate template message when OpenAI is not available
        
        Args:
            lead: Lead data dictionary
            
        Returns:
            Template message
        """
        name = lead.get('name', '')
        title = lead.get('title', '')
        company = lead.get('company', '')
        relevance_score = lead.get('relevance_score', '')
        
        template = f"""
        Subject: DuPont Tedlar Protective Films - Enhanced Durability for {company} Products

        Dear {name},

        I hope this email finds you well. As the {title} at {company}, I thought you might be interested in how DuPont Tedlar protective films can enhance the durability and performance of your products.

        {relevance_score}

        I'd appreciate the opportunity to discuss how Tedlar can specifically benefit your applications in a brief 15-minute call. Would you be available next week for a quick conversation?

        Best regards,
        Sales Representative
        DuPont Tedlar
        sales@tedlar.dupont.com
        (555) 123-4567
        """
        
        return template