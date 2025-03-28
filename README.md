# DuPont Tedlar Sales Lead Generation System

This project is an AI-powered sales lead generation system for DuPont Tedlar's Graphics & Signage team. It automates the process of identifying potential customers, finding key decision-makers, and generating personalized outreach messages.

## Features

- **Event & Association Research**: Automatically collects data from industry events and associations
- **Company Sourcing & Prioritization**: Identifies and prioritizes companies based on relevance to DuPont Tedlar
- **Stakeholder Identification**: Locates key decision-makers at target companies
- **Lead Scoring**: Evaluates and ranks potential leads based on multiple criteria
- **Personalized Outreach**: Generates tailored messages for each stakeholder
- **Interactive Dashboard**: Visualizes results in an easy-to-use format

## Installation

1. Clone this repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Create a `.env` file in the root directory with your API keys (see `.env.example` for format)

## Usage

Run the main program:

```
python run.py
```

This will:
1. Collect data from industry events and associations
2. Identify and enrich company information
3. Find key stakeholders at each company
4. Generate personalized outreach messages
5. Create and open an interactive dashboard

## Project Structure

- `src/`: Source code
  - `config/`: Configuration files
  - `data_collection/`: Modules for collecting event and company data
  - `data_enrichment/`: Modules for enriching company and stakeholder data
  - `lead_scoring/`: Lead evaluation and prioritization
  - `outreach_generation/`: Personalized message generation
  - `visualization/`: Dashboard generation
- `data/`: Data storage
  - `raw/`: Raw collected data
  - `processed/`: Intermediate processed data
  - `output/`: Final output data and dashboard
- `tests/`: Test files

## Integration Possibilities

The system is designed to be integrated with:
- LinkedIn Sales Navigator API for stakeholder identification
- Clay API for contact information enrichment
- CRM systems for lead management

## License

This project is proprietary and confidential.