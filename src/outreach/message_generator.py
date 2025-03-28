"""Message Generator Module for DuPont Tedlar Sales Lead Generation System

This module is responsible for generating personalized outreach messages
for stakeholders at target companies.
"""

import os
import logging
import pandas as pd
import openai
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.config.config import (
    OUTPUT_DATA_DIR,
    OPENAI_API_KEY,
    TARGET_PRODUCT,
    TARGET_INDUSTRY
)


class MessageGenerator:
    """Class for generating personalized outreach messages for stakeholders"""
    
    def __init__(self):
        """Initialize the MessageGenerator with OpenAI API key and settings"""
        # Set OpenAI API key
        openai.api_key = OPENAI_API_KEY
        
        # Ensure output directory exists
        self.output_dir = OUTPUT_DATA_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, 
                           format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Message templates
        self.templates = {
            'initial_outreach': {
                'subject': "DuPont Tedlar for {company}'s {industry} Applications",
                'body': """Dear {name},

I hope this message finds you well. I noticed {company}'s impressive work in the {industry} industry, particularly your {products_or_services}.

I'm reaching out because DuPont Tedlar films offer unique benefits for graphics and signage applications that might align with your needs:

- Long-lasting durability (10+ years outdoor life)
- Superior chemical resistance
- Excellent UV protection
- Graffiti resistance
- Easy cleaning and maintenance

Would you be interested in discussing how Tedlar could enhance your {specific_application} projects? I'd be happy to share some case studies or arrange a sample for you to evaluate.

Best regards,
[Your Name]
DuPont Tedlar Graphics & Signage Team
"""
            },
            'follow_up': {
                'subject': "Following up: DuPont Tedlar for {company}",
                'body': """Dear {name},

I wanted to follow up on my previous message about how DuPont Tedlar films could benefit {company}'s {industry} applications.

Recently, we've helped companies similar to yours achieve {benefit} with our Tedlar solutions. For example, {case_study}.

I'd welcome the opportunity to discuss your specific needs and how our products might address them. Would you have 15 minutes for a quick call next week?

Best regards,
[Your Name]
DuPont Tedlar Graphics & Signage Team
"""
            },
            'event_based': {
                'subject': "Meeting at {event_name}? DuPont Tedlar innovations",
                'body': """Dear {name},

I noticed that {company} will be attending {event_name} in {event_location} this {event_date}. Our DuPont Tedlar team will also be there showcasing our latest innovations for the {industry} industry.

Given your role as {title} and {company}'s focus on {products_or_services}, I thought you might be interested in seeing how our Tedlar films can provide superior protection and longevity for graphics and signage applications.

Would you be available for a brief meeting at the event? I'd be happy to schedule a specific time that works for your agenda.

Best regards,
[Your Name]
DuPont Tedlar Graphics & Signage Team
"""
            }
        }
    
    def generate_messages(self, stakeholders_df, companies_df, events_df=None):
        """Generate personalized outreach messages for stakeholders
        
        Args:
            stakeholders_df (pandas.DataFrame): DataFrame containing stakeholder information
            companies_df (pandas.DataFrame): DataFrame containing company information
            events_df (pandas.DataFrame, optional): DataFrame containing event information
            
        Returns:
            pandas.DataFrame: DataFrame containing stakeholders with personalized messages
        """
        self.logger.info(f"Generating messages for {len(stakeholders_df)} stakeholders")
        
        # Merge stakeholders with company information
        stakeholders_with_companies = pd.merge(
            stakeholders_df,
            companies_df[['name', 'industry', 'description', 'products', 'target_markets']],
            left_on='company',
            right_on='name',
            suffixes=('', '_company')
        )
        
        # Generate messages for each stakeholder
        messages = []
        
        for _, stakeholder in stakeholders_with_companies.iterrows():
            # Determine the best template based on stakeholder and company information
            template_type = self._select_template_type(stakeholder, events_df)
            
            # Generate personalized message
            message = self._generate_personalized_message(stakeholder, template_type, events_df)
            
            messages.append({
                'stakeholder_id': stakeholder.get('id', ''),
                'name': stakeholder['name'],
                'company': stakeholder['company'],
                'email': stakeholder.get('email', ''),
                'template_type': template_type,
                'subject': message['subject'],
                'body': message['body']
            })
        
        # Create DataFrame from messages
        messages_df = pd.DataFrame(messages)
        
        # Save messages to CSV
        messages_df.to_csv(self.output_dir / 'stakeholders_with_messages.csv', index=False)
        self.logger.info(f"Saved {len(messages_df)} messages to stakeholders_with_messages.csv")
        
        # Merge messages back with stakeholders DataFrame
        stakeholders_with_messages = pd.merge(
            stakeholders_df,
            messages_df[['stakeholder_id', 'template_type', 'subject', 'body']],
            left_on='id',
            right_on='stakeholder_id',
            how='left'
        )
        
        return stakeholders_with_messages
    
    def _select_template_type(self, stakeholder, events_df):
        """Select the best template type for a stakeholder
        
        Args:
            stakeholder (pandas.Series): Stakeholder information
            events_df (pandas.DataFrame): DataFrame containing event information
            
        Returns:
            str: Template type ('initial_outreach', 'follow_up', or 'event_based')
        """
        # If events data is available and the company is attending an event, use event-based template
        if events_df is not None and not events_df.empty:
            # This is a simplified check - in a real system, we would have a more sophisticated way
            # to determine if a company is attending an event
            company_name = stakeholder['company'].lower()
            
            for _, event in events_df.iterrows():
                # Check if we have exhibitor information for this event
                if 'exhibitors' in event and company_name in [e.lower() for e in event['exhibitors']]:
                    return 'event_based'
        
        # For high-priority stakeholders, use initial outreach
        if 'priority_score' in stakeholder and stakeholder['priority_score'] >= 0.7:
            return 'initial_outreach'
        
        # Default to follow-up template
        return 'follow_up'
    
    def _generate_personalized_message(self, stakeholder, template_type, events_df):
        """Generate a personalized message for a stakeholder using a template
        
        Args:
            stakeholder (pandas.Series): Stakeholder information
            template_type (str): Template type ('initial_outreach', 'follow_up', or 'event_based')
            events_df (pandas.DataFrame): DataFrame containing event information
            
        Returns:
            dict: Dictionary containing subject and body of the message
        """
        template = self.templates[template_type]
        
        # Extract relevant information for message personalization
        name = stakeholder['name']
        company = stakeholder['company']
        title = stakeholder.get('title', 'professional')
        industry = stakeholder.get('industry', TARGET_INDUSTRY)
        
        # Extract products or services from company information
        products_or_services = 'products and services'
        if 'products' in stakeholder and stakeholder['products']:
            if isinstance(stakeholder['products'], list):
                products_or_services = ', '.join(stakeholder['products'][:2])
            else:
                products_or_services = str(stakeholder['products'])
        
        # Determine specific application based on industry and products
        specific_application = 'signage and graphics'
        if 'target_markets' in stakeholder and stakeholder['target_markets']:
            if isinstance(stakeholder['target_markets'], list):
                specific_application = stakeholder['target_markets'][0]
            else:
                specific_application = str(stakeholder['target_markets'])
        
        # Prepare message variables
        message_vars = {
            'name': name,
            'company': company,
            'title': title,
            'industry': industry,
            'products_or_services': products_or_services,
            'specific_application': specific_application,
            'benefit': 'improved durability and reduced maintenance costs',
            'case_study': 'a major retailer was able to reduce signage replacement by 40% after switching to Tedlar-protected graphics'
        }
        
        # Add event information if using event-based template
        if template_type == 'event_based' and events_df is not None and not events_df.empty:
            # Use the highest relevance score event
            event = events_df.sort_values('relevance_score', ascending=False).iloc[0]
            
            message_vars.update({
                'event_name': event['name'],
                'event_location': event['location'],
                'event_date': event['date']
            })
        
        # Format subject and body with variables
        subject = template['subject'].format(**message_vars)
        body = template['body'].format(**message_vars)
        
        # For more sophisticated personalization, we could use OpenAI API
        # to generate completely custom messages based on stakeholder information
        if OPENAI_API_KEY and 'priority_score' in stakeholder and stakeholder['priority_score'] >= 0.9:
            try:
                custom_message = self._generate_ai_message(stakeholder, message_vars)
                if custom_message:
                    return custom_message
            except Exception as e:
                self.logger.error(f"Error generating AI message: {str(e)}")
        
        return {
            'subject': subject,
            'body': body
        }
    
    def _generate_ai_message(self, stakeholder, message_vars):
        """Generate a completely custom message using OpenAI API
        
        Args:
            stakeholder (pandas.Series): Stakeholder information
            message_vars (dict): Variables for message personalization
            
        Returns:
            dict: Dictionary containing subject and body of the message, or None if generation fails
        """
        try:
            # Prepare prompt for OpenAI API
            prompt = f"""Generate a personalized sales outreach email for a potential customer of DuPont Tedlar films for graphics and signage applications.

Recipient Information:
- Name: {message_vars['name']}
- Title: {message_vars['title']}
- Company: {message_vars['company']}
- Industry: {message_vars['industry']}
- Company Products/Services: {message_vars['products_or_services']}
- Specific Application: {message_vars['specific_application']}

Product Information:
- DuPont Tedlar films provide superior protection for graphics and signage
- Benefits include long-lasting durability (10+ years outdoor life), chemical resistance, UV protection, graffiti resistance, and easy cleaning

Generate a subject line and email body that is professional, concise, personalized, and compelling. Focus on how Tedlar can solve specific problems for this company based on their industry and applications.

Format your response as JSON with 'subject' and 'body' fields.
"""
            
            # Call OpenAI API
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that generates personalized sales outreach emails."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=500
            )
            
            # Extract message from response
            message_text = response.choices[0].message.content
            
            # Parse JSON response
            import json
            try:
                message_json = json.loads(message_text)
                return {
                    'subject': message_json['subject'],
                    'body': message_json['body']
                }
            except json.JSONDecodeError:
                # If JSON parsing fails, extract subject and body using regex
                import re
                subject_match = re.search(r'Subject: (.+)', message_text)
                body_match = re.search(r'Body: (.+)', message_text, re.DOTALL)
                
                if subject_match and body_match:
                    return {
                        'subject': subject_match.group(1).strip(),
                        'body': body_match.group(1).strip()
                    }
                else:
                    return None
        
        except Exception as e:
            self.logger.error(f"Error in AI message generation: {str(e)}")
            return None