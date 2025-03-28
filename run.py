#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
DuPont Tedlar Sales Lead Generation System

This script is the main entry point for the DuPont Tedlar Sales Lead Generation System.
It orchestrates the entire process of collecting data, enriching it, scoring leads,
and generating a dashboard for visualization.
"""

import os
import logging
import pandas as pd
from pathlib import Path
import webbrowser

# Import modules from the project
from src.data_collection.event_scraper import EventScraper
from src.data_collection.company_scraper import CompanyScraper
from src.data_enrichment.company_enricher import CompanyEnricher
from src.data_enrichment.stakeholder_finder import StakeholderFinder
from src.lead_scoring.lead_scorer import LeadScorer
from src.outreach_generation.message_generator import MessageGenerator
from src.visualization.dashboard_generator import DashboardGenerator
from src.config.config import OUTPUT_DATA_DIR


def main():
    """Main function to run the entire lead generation process"""
    # Setup logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    
    logger.info("Starting DuPont Tedlar Sales Lead Generation System")
    
    # Step 1: Collect event and association data
    logger.info("Step 1: Collecting event and association data")
    event_scraper = EventScraper()
    events_df = event_scraper.get_events_data()
    associations_df = event_scraper.get_associations_data()
    
    # Step 2: Collect company data from events and associations
    logger.info("Step 2: Collecting company data")
    company_scraper = CompanyScraper()
    companies_from_events_df = company_scraper.scrape_companies_from_events(events_df)
    companies_from_associations_df = company_scraper.scrape_companies_from_associations(associations_df)
    
    # Combine company data from both sources
    companies_df = pd.concat([companies_from_events_df, companies_from_associations_df])
    companies_df = companies_df.drop_duplicates(subset=['name', 'website'])
    logger.info(f"Collected data for {len(companies_df)} unique companies")
    
    # Step 3: Enrich company data
    logger.info("Step 3: Enriching company data")
    company_enricher = CompanyEnricher()
    enriched_companies_df = company_enricher.enrich_companies(companies_df)
    
    # Step 4: Find stakeholders at target companies
    logger.info("Step 4: Finding stakeholders at target companies")
    stakeholder_finder = StakeholderFinder()
    stakeholders_df = stakeholder_finder.find_stakeholders(enriched_companies_df)
    
    # Step 5: Score and prioritize leads
    logger.info("Step 5: Scoring and prioritizing leads")
    lead_scorer = LeadScorer()
    scored_companies_df, scored_stakeholders_df, leads_df = lead_scorer.score_leads(
        enriched_companies_df, stakeholders_df
    )
    
    # Step 6: Generate personalized outreach messages
    logger.info("Step 6: Generating personalized outreach messages")
    message_generator = MessageGenerator()
    stakeholders_with_messages_df = message_generator.generate_messages(
        scored_stakeholders_df, scored_companies_df, events_df
    )
    
    # Save leads with messages
    # Ensure we have the correct join key
    if 'stakeholder_id' in leads_df.columns:
        join_key = 'stakeholder_id'
    elif 'id' in leads_df.columns:
        join_key = 'id'
    else:
        # If neither key exists, create a simple concatenation
        logger.warning("No matching join key found between leads and messages. Creating simple output.")
        leads_with_messages_df = leads_df.copy()
        # Add empty message columns if needed
        if 'subject' not in leads_with_messages_df.columns:
            leads_with_messages_df['subject'] = ''
        if 'body' not in leads_with_messages_df.columns:
            leads_with_messages_df['body'] = ''
    
    # If we have a valid join key, perform the merge
    if 'join_key' in locals():
        leads_with_messages_df = pd.merge(
            leads_df,
            stakeholders_with_messages_df[['id', 'subject', 'body']],
            left_on=join_key,
            right_on='id',
            how='left'
        )
    leads_with_messages_df.to_csv(OUTPUT_DATA_DIR / 'leads_with_messages.csv', index=False)
    
    # Step 7: Generate interactive dashboard
    logger.info("Step 7: Generating interactive dashboard")
    dashboard_generator = DashboardGenerator()
    dashboard_path = dashboard_generator.generate_dashboard(
        leads_df, scored_companies_df, scored_stakeholders_df
    )
    
    # Open dashboard in web browser
    logger.info(f"Opening dashboard at {dashboard_path}")
    # Convert to absolute path and ensure proper URL format
    absolute_path = os.path.abspath(dashboard_path)
    webbrowser.open(f'file://{absolute_path}')
    
    logger.info("DuPont Tedlar Sales Lead Generation System completed successfully")
    logger.info(f"Results saved to {OUTPUT_DATA_DIR}")


if __name__ == "__main__":
    main()