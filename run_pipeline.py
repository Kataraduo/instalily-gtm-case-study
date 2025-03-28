"""
Main script - Execute the entire data processing and dashboard generation pipeline
"""
import logging
import pandas as pd
from pathlib import Path
from src.data_collection.event_scraper import EventScraper
from src.data_collection.association_scraper import AssociationScraper
from src.data_collection.company_scraper import CompanyScraper
from src.data_enrichment.company_enricher import CompanyEnricher
from src.data_enrichment.stakeholder_finder import StakeholderFinder
from src.outreach.message_generator import MessageGenerator
from src.visualization.dashboard_generator import DashboardGenerator
from src.config.config import OUTPUT_DATA_DIR

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main function to execute the entire data processing and dashboard generation pipeline"""
    logger.info("Starting data processing and dashboard generation pipeline")
    
    # Create output directory
    output_dir = Path(OUTPUT_DATA_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Collect event data
    logger.info("Step 1: Collecting event data")
    event_scraper = EventScraper()
    events_df = event_scraper.get_events_data()
    events_df.to_csv(output_dir / "events.csv", index=False)
    logger.info(f"Collected {len(events_df)} events")
    
    # Step 2: Collect association data
    logger.info("Step 2: Collecting association data")
    association_scraper = AssociationScraper()
    associations_df = association_scraper.collect_associations_data()
    associations_df.to_csv(output_dir / "associations.csv", index=False)
    logger.info(f"Collected {len(associations_df)} associations")
    
    # Step 3: Collect company data
    logger.info("Step 3: Collecting company data")
    company_scraper = CompanyScraper()
    companies_df = company_scraper.collect_companies_data(events_df, associations_df)  # Using the newly added method
    companies_df.to_csv(output_dir / "companies.csv", index=False)
    logger.info(f"Collected {len(companies_df)} companies")
    
    # Step 4: Enrich company data
    logger.info("Step 4: Enriching company data")
    company_enricher = CompanyEnricher()
    enriched_companies_df = company_enricher.enrich_companies(companies_df)
    enriched_companies_df.to_csv(output_dir / "enriched_companies.csv", index=False)
    logger.info(f"Enriched data for {len(enriched_companies_df)} companies")
    
    # Step 5: Find stakeholders
    logger.info("Step 5: Finding stakeholders")
    stakeholder_finder = StakeholderFinder()
    stakeholders_df = stakeholder_finder.find_stakeholders(enriched_companies_df)
    stakeholders_df.to_csv(output_dir / "stakeholders.csv", index=False)
    logger.info(f"Found {len(stakeholders_df)} stakeholders")
    
    # Step 6: Score leads
    logger.info("Step 6: Scoring leads")
    from src.lead_scoring.lead_scorer import LeadScorer
    lead_scorer = LeadScorer()
    scored_companies_df, scored_stakeholders_df, leads_df = lead_scorer.score_leads(enriched_companies_df, stakeholders_df)
    leads_df.to_csv(output_dir / "scored_leads.csv", index=False)
    logger.info(f"Scored {len(leads_df)} leads")
    
    # Step 7: Generate outreach messages
    logger.info("Step 7: Generating outreach messages")
    message_generator = MessageGenerator()
    stakeholders_with_messages_df = message_generator.generate_messages(scored_stakeholders_df, scored_companies_df)
    stakeholders_with_messages_df.to_csv(output_dir / "stakeholders_with_messages.csv", index=False)
    logger.info(f"Generated outreach messages for {len(stakeholders_with_messages_df)} stakeholders")
    
    # Step 8: Generate dashboard
    logger.info("Step 8: Generating dashboard")
    dashboard_generator = DashboardGenerator()
    dashboard_path = dashboard_generator.generate_dashboard(leads_df, scored_companies_df, scored_stakeholders_df)
    logger.info(f"Dashboard generated: {dashboard_path}")
    
    logger.info("Data processing and dashboard generation pipeline completed")
    logger.info(f"All output files saved in: {output_dir}")
    logger.info(f"Dashboard path: {dashboard_path}")
    
    # Return dashboard path for browser opening
    return dashboard_path

if __name__ == "__main__":
    dashboard_path = main()
    print(f"\nDashboard generated. Please open the following file in your browser:\n{dashboard_path}")
