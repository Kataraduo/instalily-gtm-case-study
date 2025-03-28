import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.outreach_generation.message_generator import MessageGenerator


@pytest.fixture
def message_generator():
    """Create a MessageGenerator instance for testing"""
    return MessageGenerator()


@pytest.fixture
def mock_stakeholders_df():
    """Create a mock stakeholders DataFrame for testing"""
    stakeholders_data = {
        'name': ['John Doe', 'Jane Smith'],
        'title': ['CEO', 'CTO'],
        'company': ['Company A', 'Company B'],
        'email': ['john@companya.com', 'jane@companyb.com'],
        'linkedin_url': ['https://linkedin.com/in/johndoe', 'https://linkedin.com/in/janesmith'],
        'decision_making_power': [0.8, 0.7],
        'relevance_score': [0.9, 0.8],
        'priority_score': [0.85, 0.75]
    }
    return pd.DataFrame(stakeholders_data)


@pytest.fixture
def mock_companies_df():
    """Create a mock companies DataFrame for testing"""
    companies_data = {
        'name': ['Company A', 'Company B'],
        'website': ['https://companya.com', 'https://companyb.com'],
        'industry': ['Manufacturing', 'Technology'],
        'description': ['Manufactures solar panels', 'Produces electronic devices'],
        'products': [['Solar Panels', 'Inverters'], ['Smartphones', 'Tablets']],
        'materials': [['Glass', 'Silicon'], ['Plastic', 'Metal']],
        'target_markets': [['Residential', 'Commercial'], ['Consumer', 'Business']],
        'company_size': ['Medium', 'Large'],
        'annual_revenue': ['$10M-$50M', '$100M-$500M']
    }
    return pd.DataFrame(companies_data)


def test_init(message_generator):
    """Test MessageGenerator initialization"""
    assert message_generator is not None
    assert hasattr(message_generator, 'openai_api_key')
    assert hasattr(message_generator, 'model')


def test_generate_template_message(message_generator, mock_stakeholders_df):
    """Test template message generation when no API key is available"""
    # Set API key to empty to force template message generation
    message_generator.openai_api_key = ""
    
    # Get the first stakeholder
    lead = mock_stakeholders_df.iloc[0].to_dict()
    
    # Generate template message
    message = message_generator._generate_template_message(lead)
    
    # Check that the message contains the stakeholder's name and company
    assert isinstance(message, str)
    assert lead['name'] in message
    assert lead['company'] in message
    assert "I'm reaching out" in message


@patch('openai.OpenAI')
def test_generate_personalized_message_with_api(mock_openai, message_generator, mock_stakeholders_df):
    """Test personalized message generation with OpenAI API"""
    # Set up mock OpenAI client
    mock_client = MagicMock()
    mock_openai.return_value = mock_client
    
    # Set up mock response
    mock_completion = MagicMock()
    mock_completion.choices = [MagicMock(message=MagicMock(content="Test personalized message"))]
    mock_client.chat.completions.create.return_value = mock_completion
    
    # Set API key to a non-empty value
    message_generator.openai_api_key = "test_api_key"
    
    # Get the first stakeholder
    lead = mock_stakeholders_df.iloc[0].to_dict()
    
    # Generate personalized message
    message = message_generator.generate_personalized_message(lead)
    
    # Check that the OpenAI API was called
    mock_client.chat.completions.create.assert_called_once()
    assert message == "Test personalized message"


@patch.object(MessageGenerator, 'generate_personalized_message')
def test_generate_messages_for_stakeholders(mock_generate, message_generator, mock_stakeholders_df, mock_companies_df):
    """Test generating messages for all stakeholders"""
    # Set up mock to return a test message
    mock_generate.return_value = "Test message for {name}"
    
    # Generate messages for stakeholders
    result_df = message_generator.generate_messages_for_stakeholders(mock_stakeholders_df, mock_companies_df)
    
    # Check the result
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == len(mock_stakeholders_df)
    assert 'outreach_message' in result_df.columns
    assert mock_generate.call_count == len(mock_stakeholders_df)


def test_prepare_prompt(message_generator, mock_stakeholders_df, mock_companies_df):
    """Test prompt preparation for OpenAI API"""
    # Get the first stakeholder
    lead = mock_stakeholders_df.iloc[0].to_dict()
    
    # Get the corresponding company
    company_info = mock_companies_df[mock_companies_df['name'] == lead['company']].iloc[0].to_dict()
    
    # Add company info to lead
    lead.update({
        'company_info': company_info
    })
    
    # Generate prompt
    prompt = message_generator._prepare_prompt(lead)
    
    # Check that the prompt contains relevant information
    assert isinstance(prompt, str)
    assert lead['name'] in prompt
    assert lead['company'] in prompt
    assert lead['title'] in prompt
    assert 'personalized outreach message' in prompt.lower()