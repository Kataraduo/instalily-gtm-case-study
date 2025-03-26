"""
Main program - Integrates all modules and runs the complete process
"""
import pandas as pd
import logging
import os
from pathlib import Path

# Modified import statements to use correct relative imports
from src.data_collection.event_scraper import EventScraper
from src.data_collection.company_scraper import CompanyScraper
from src.data_enrichment.company_enricher import CompanyEnricher
from src.data_enrichment.stakeholder_finder import StakeholderFinder
from src.lead_scoring.lead_scorer import LeadScorer
from src.outreach_generation.message_generator import MessageGenerator
from src.config.config import OUTPUT_DATA_DIR
from .visualization.dashboard_generator import DashboardGenerator
import webbrowser
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Run the complete sales lead generation process"""
    logger.info("Starting sales lead generation process")
    
    # 1. Collect industry events and associations data
    logger.info("Step 1: Collecting industry events and associations data")
    event_scraper = EventScraper()
    events_df = event_scraper.get_events_data()
    associations_df = event_scraper.get_associations_data()
    
    # Save raw data
    events_df.to_csv(OUTPUT_DATA_DIR / "events.csv", index=False)
    associations_df.to_csv(OUTPUT_DATA_DIR / "associations.csv", index=False)
    
    # 2. Collect company data
    logger.info("Step 2: Collecting company data")
    company_scraper = CompanyScraper()
    companies_df = company_scraper.collect_companies_data(events_df, associations_df)
    companies_df.to_csv(OUTPUT_DATA_DIR / "companies_raw.csv", index=False)
    
    # 3. Enrich company data
    logger.info("Step 3: Enriching company data")
    company_enricher = CompanyEnricher()
    enriched_companies_df = company_enricher.enrich_companies_data(companies_df)
    enriched_companies_df.to_csv(OUTPUT_DATA_DIR / "companies_enriched.csv", index=False)
    
    # 4. Find decision makers
    logger.info("Step 4: Finding decision makers")
    stakeholder_finder = StakeholderFinder()
    stakeholders_df = stakeholder_finder.find_stakeholders_for_companies(enriched_companies_df)
    prioritized_stakeholders_df = stakeholder_finder.prioritize_stakeholders(stakeholders_df)
    prioritized_stakeholders_df.to_csv(OUTPUT_DATA_DIR / "stakeholders.csv", index=False)
    
    # 5. Generate outreach messages
    logger.info("Step 5: Generating outreach messages")
    message_generator = MessageGenerator()
    leads_with_messages_df = message_generator.generate_messages_for_stakeholders(
        prioritized_stakeholders_df, enriched_companies_df
    )
    leads_with_messages_df.to_csv(OUTPUT_DATA_DIR / "leads_with_messages.csv", index=False)
    
    # 6. Generate final dashboard data
    logger.info("Step 6: Generating final dashboard data")
    # Merge all data for final output
    dashboard_data = leads_with_messages_df.sort_values(by='priority_score', ascending=False)
    dashboard_data.to_csv(OUTPUT_DATA_DIR / "dashboard_data.csv", index=False)
    
    logger.info(f"Process completed! Generated {len(dashboard_data)} potential sales leads")
    logger.info(f"Results saved at: {OUTPUT_DATA_DIR}")
    
    # Display preview of top 5 leads
    print("\nPreview of top 5 high-priority sales leads:")
    preview_columns = ['name', 'title', 'company', 'priority_score', 'linkedin_url']
    print(dashboard_data[preview_columns].head(5))
    
    # Generate outreach messages
    message_generator = MessageGenerator()
    stakeholders_df_with_messages = message_generator.generate_messages_for_stakeholders(stakeholders_df, enriched_companies_df)
    
    # Save results
    stakeholders_df_with_messages.to_csv(OUTPUT_DATA_DIR / "stakeholders_with_messages.csv", index=False)
    logger.info(f"Saved stakeholders with messages to {OUTPUT_DATA_DIR / 'stakeholders_with_messages.csv'}")
    
    # Generate dashboard
    dashboard_generator = DashboardGenerator()
    dashboard_path = dashboard_generator.generate_dashboard(stakeholders_df_with_messages, enriched_companies_df)
    logger.info(f"Generated dashboard at {dashboard_path}")
    
    # Automatically open dashboard
    webbrowser.open('file://' + os.path.abspath(dashboard_path))
    
    logger.info("Process completed successfully")

if __name__ == "__main__":
    main()