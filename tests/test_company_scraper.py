import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from bs4 import BeautifulSoup
import requests
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data_collection.company_scraper import CompanyScraper


@pytest.fixture
def company_scraper():
    """Create a CompanyScraper instance for testing"""
    return CompanyScraper()


@pytest.fixture
def mock_events_df():
    """Create a mock events DataFrame for testing"""
    events_data = [
        {
            'name': 'ISA Sign Expo 2023',
            'url': 'https://www.signexpo.org/',
            'date': '2023-04-12',
            'location': 'Las Vegas, NV',
            'description': 'International Sign Association Expo',
            'relevance_score': 0.95
        },
        {
            'name': 'PRINTING United Expo',
            'url': 'https://www.printingunited.com/',
            'date': '2023-10-18',
            'location': 'Atlanta, GA',
            'description': 'Printing, graphics and signage industry expo',
            'relevance_score': 0.92
        }
    ]
    return pd.DataFrame(events_data)


@pytest.fixture
def mock_associations_df():
    """Create a mock associations DataFrame for testing"""
    associations_data = [
        {
            'name': 'SGIA - Specialty Graphic Imaging Association',
            'url': 'https://www.sgia.org/',
            'description': 'Association for printing, graphics and visual communications',
            'relevance_score': 0.90
        },
        {
            'name': 'ISA - International Sign Association',
            'url': 'https://www.signs.org/',
            'description': 'Trade association for sign industry professionals',
            'relevance_score': 0.95
        }
    ]
    return pd.DataFrame(associations_data)


def test_init(company_scraper):
    """Test CompanyScraper initialization"""
    assert company_scraper is not None
    assert company_scraper.headers is not None
    assert 'User-Agent' in company_scraper.headers
    assert company_scraper.industry_keywords is not None


@patch('requests.get')
def test_collect_companies_data(mock_get, company_scraper, mock_events_df, mock_associations_df):
    """Test collect_companies_data method"""
    # Mock response for requests.get
    mock_response = MagicMock()
    mock_response.text = '<html><body>Test HTML</body></html>'
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    # Mock the methods that would be called
    with patch.object(CompanyScraper, '_get_companies_from_event', return_value=[{'name': 'Test Company 1'}]):
        with patch.object(CompanyScraper, '_get_companies_from_association', return_value=[{'name': 'Test Company 2'}]):
            # Call the method
            result = company_scraper.collect_companies_data(mock_events_df, mock_associations_df)
            
            # Check the result
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2  # One from event, one from association
            assert 'name' in result.columns
            assert 'Test Company 1' in result['name'].values
            assert 'Test Company 2' in result['name'].values


@patch('requests.get')
def test_get_companies_from_event(mock_get, company_scraper):
    """Test _get_companies_from_event method"""
    # Mock response for requests.get
    mock_response = MagicMock()
    mock_response.text = '<html><body><a href="/exhibitors">Exhibitors</a></body></html>'
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    # Create a test event
    event = {
        'name': 'Test Event',
        'url': 'https://test-event.com',
        'date': '2023-01-01',
        'location': 'Test Location',
        'description': 'Test Description',
        'relevance_score': 0.9
    }
    
    # Mock the methods that would be called
    with patch.object(CompanyScraper, '_get_companies_from_floor_plan', return_value=[]):
        with patch.object(CompanyScraper, '_get_companies_from_alternative_sources', return_value=[]):
            # Call the method
            result = company_scraper._get_companies_from_event(event)
            
            # Check that requests.get was called with the event URL
            mock_get.assert_called_with('https://test-event.com', headers=company_scraper.headers, timeout=10)


@patch('requests.get')
def test_get_companies_from_floor_plan(mock_get, company_scraper):
    """Test _get_companies_from_floor_plan method"""
    # Mock response for requests.get
    mock_response = MagicMock()
    mock_response.text = '<html><body><a href="/floorplan">Floor Plan</a></body></html>'
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    # Create a test event
    event = {
        'name': 'Test Event',
        'url': 'https://test-event.com',
        'date': '2023-01-01',
        'location': 'Test Location',
        'description': 'Test Description',
        'relevance_score': 0.9
    }
    
    # Mock the methods that would be called
    with patch.object(CompanyScraper, '_parse_mapyourshow_floor_plan', return_value=[{'name': 'Floor Plan Company'}]):
        with patch.object(CompanyScraper, '_parse_generic_floor_plan', return_value=[]):
            # Call the method
            result = company_scraper._get_companies_from_floor_plan(event)
            
            # Check the result
            assert len(result) == 1
            assert result[0]['name'] == 'Floor Plan Company'


def test_parse_mapyourshow_floor_plan(company_scraper):
    """Test _parse_mapyourshow_floor_plan method"""
    # Create a test soup object
    html = '''
    <html>
        <body>
            <div class="exhibitor-list">
                <div class="exhibitor">
                    <h3>Test Exhibitor 1</h3>
                    <a href="http://www.testexhibitor1.com">Website</a>
                    <div>Booth: A123</div>
                </div>
                <div class="exhibitor">
                    <h3>Test Exhibitor 2</h3>
                    <a href="http://www.testexhibitor2.com">Website</a>
                    <div>Booth: B456</div>
                </div>
            </div>
        </body>
    </html>
    '''
    soup = BeautifulSoup(html, 'html.parser')
    
    # Create a test event
    event = {
        'name': 'Test Event',
        'url': 'https://test-event.com',
        'date': '2023-01-01',
        'location': 'Test Location',
        'description': 'Test Description',
        'relevance_score': 0.9
    }
    
    # Call the method
    with patch('requests.get'):
        result = company_scraper._parse_mapyourshow_floor_plan(soup, 'https://test-event.com/floorplan', event)
    
    # Check the result
    assert len(result) == 2
    assert any(company['name'] == 'Test Exhibitor 1' for company in result)
    assert any(company['name'] == 'Test Exhibitor 2' for company in result)


def test_parse_generic_floor_plan(company_scraper):
    """Test _parse_generic_floor_plan method"""
    # Create a test soup object
    html = '''
    <html>
        <body>
            <div class="booth-list">
                <div class="booth">
                    <span>Generic Exhibitor 1</span>
                    <span>Booth: C789</span>
                </div>
                <div class="booth">
                    <span>Generic Exhibitor 2</span>
                    <span>Booth: D012</span>
                </div>
            </div>
        </body>
    </html>
    '''
    soup = BeautifulSoup(html, 'html.parser')
    
    # Create a test event
    event = {
        'name': 'Test Event',
        'url': 'https://test-event.com',
        'date': '2023-01-01',
        'location': 'Test Location',
        'description': 'Test Description',
        'relevance_score': 0.9
    }
    
    # Call the method
    result = company_scraper._parse_generic_floor_plan(soup, 'https://test-event.com/floorplan', event)
    
    # The method should extract company information from the generic floor plan
    # This is a simplified test as the actual implementation would be more complex
    assert isinstance(result, list)